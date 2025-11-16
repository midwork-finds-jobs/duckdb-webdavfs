#include "webdavfs.hpp"

#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "http_state.hpp"
#endif

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "httpfs_client.hpp"
#include "httpfs_curl_client.hpp"

#include <fstream>
#include <cstdlib>
#include <unistd.h>
#include <sys/stat.h>

namespace duckdb {

// Global debug logging flag (set per-client during initialization)
// This is a workaround since the debug macro is used in places without easy access to HTTPParams
static thread_local bool g_webdav_debug_enabled = false;

// Debug logging macro - uses thread-local flag set during handle initialization
#define WEBDAV_DEBUG_LOG(...)                                                                                          \
	do {                                                                                                               \
		if (g_webdav_debug_enabled) {                                                                                  \
			fprintf(stderr, __VA_ARGS__);                                                                              \
			fflush(stderr);                                                                                            \
		}                                                                                                              \
	} while (0)

WebDAVFileHandle::~WebDAVFileHandle() {
	// Clean up temp file if it exists
	if (using_temp_file && !temp_file_path.empty()) {
		std::remove(temp_file_path.c_str());
	}
}

void WebDAVFileHandle::Close() {
	WEBDAV_DEBUG_LOG("[WebDAV] Close called for: %s\n", path.c_str());
	FlushBuffer();

	// Clean up temp file after successful flush
	if (using_temp_file && !temp_file_path.empty()) {
		std::remove(temp_file_path.c_str());
		temp_file_path.clear();
		using_temp_file = false;
	}
}

void WebDAVFileHandle::FlushBuffer() {
	if (!buffer_dirty && !using_temp_file) {
		WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: nothing to flush (dirty=%d, using_temp=%d)\n", buffer_dirty,
		                 using_temp_file);
		return;
	}

	auto &webdav_fs = dynamic_cast<WebDAVFileSystem &>(file_system);
	auto parsed_url = webdav_fs.ParseUrl(path);
	string http_url = parsed_url.GetHTTPUrl();

	HTTPHeaders headers;
	unique_ptr<HTTPResponse> response;

	if (using_temp_file) {
		// Append any remaining buffer to temp file
		if (!write_buffer.empty()) {
			FILE *fp = fopen(temp_file_path.c_str(), "ab");
			if (fp) {
				fwrite(write_buffer.data(), 1, write_buffer.size(), fp);
				fclose(fp);
				write_buffer.clear();
			}
		}

		// Get file size
		struct stat st;
		if (stat(temp_file_path.c_str(), &st) != 0) {
			throw IOException("Failed to stat temp file %s", temp_file_path);
		}

		WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: streaming upload from temp file %s (%lld bytes)\n",
		                 temp_file_path.c_str(), (long long)st.st_size);

		// Stream upload from temp file
		response = webdav_fs.PutRequestFromFile(*this, http_url, headers, temp_file_path, st.st_size);
	} else {
		// Small file: upload from memory buffer
		WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: uploading %zu bytes from memory\n", write_buffer.size());
		response = webdav_fs.PutRequest(*this, http_url, headers, const_cast<char *>(write_buffer.data()),
		                                write_buffer.size(), "");
	}

	WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: PUT returned %d\n", static_cast<int>(response->status));

	// If write failed with 400, 404, or 409, try to create parent directories and retry
	if (response->status == HTTPStatusCode::BadRequest_400 || response->status == HTTPStatusCode::NotFound_404 ||
	    response->status == HTTPStatusCode::Conflict_409) {
		WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: Got error %d, trying to create parent directories\n",
		                 static_cast<int>(response->status));

		// Extract directory path from file path
		auto last_slash = path.rfind('/');
		if (last_slash != string::npos) {
			string dir_path = path.substr(0, last_slash);

			try {
				webdav_fs.CreateDirectoryRecursive(dir_path);
				// Retry the write after directory creation
				response = webdav_fs.PutRequest(*this, http_url, headers, const_cast<char *>(write_buffer.data()),
				                                write_buffer.size(), "");
				WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: Retry PUT returned %d\n", static_cast<int>(response->status));
			} catch (const std::exception &e) {
				// If directory creation fails, continue with original error
			}
		}
	}

	if (response->status != HTTPStatusCode::OK_200 && response->status != HTTPStatusCode::Created_201 &&
	    response->status != HTTPStatusCode::NoContent_204) {
		throw IOException("Failed to write to file %s: HTTP %d", path, static_cast<int>(response->status));
	}

	// Clear the buffer after successful write
	write_buffer.clear();
	buffer_dirty = false;
	WEBDAV_DEBUG_LOG("[WebDAV] FlushBuffer: successfully flushed and cleared buffer\n");
}

void WebDAVFileHandle::Initialize(optional_ptr<FileOpener> opener) {
	HTTPFileHandle::Initialize(opener);
	// Set thread-local debug flag from settings
	auto &httpfs_params = dynamic_cast<HTTPFSParams &>(http_params);
	g_webdav_debug_enabled = httpfs_params.webdav_debug_logging;
}

unique_ptr<HTTPClient> WebDAVFileHandle::CreateClient() {
	WEBDAV_DEBUG_LOG("[WebDAV] CreateClient called, http_util name: %s\n", http_params.http_util.GetName().c_str());
	fflush(stderr);
	auto client = http_params.http_util.InitializeClient(http_params, path);
	WEBDAV_DEBUG_LOG("[WebDAV] CreateClient returned client: %p\n", (void *)client.get());
	fflush(stderr);
	return client;
}

WebDAVAuthParams WebDAVAuthParams::ReadFrom(optional_ptr<FileOpener> opener, FileOpenerInfo &info) {
	WebDAVAuthParams params;

	if (!opener) {
		return params;
	}

	KeyValueSecretReader secret_reader(*opener, &info, "webdav");
	secret_reader.TryGetSecretKey("username", params.username);
	secret_reader.TryGetSecretKey("password", params.password);

	return params;
}

string ParsedWebDAVUrl::GetHTTPUrl() const {
	return http_proto + "://" + host + path;
}

ParsedWebDAVUrl WebDAVFileSystem::ParseUrl(const string &url) {
	ParsedWebDAVUrl result;

	// Check for storagebox:// protocol (Hetzner Storage Box shorthand)
	if (StringUtil::StartsWith(url, "storagebox://")) {
		result.http_proto = "https";
		// Extract username and path from storagebox://u123456/path/to/file
		string remainder = url.substr(13); // Skip "storagebox://"

		auto slash_pos = remainder.find('/');
		string username;
		if (slash_pos != string::npos) {
			username = remainder.substr(0, slash_pos);
			result.path = remainder.substr(slash_pos);
		} else {
			username = remainder;
			result.path = "/";
		}

		// Build the Hetzner Storage Box hostname
		result.host = username + ".your-storagebox.de";
		return result;
	}

	// Check for webdav:// or webdavs:// protocol
	if (StringUtil::StartsWith(url, "webdav://")) {
		result.http_proto = "http";
		result.host = url.substr(9);
	} else if (StringUtil::StartsWith(url, "webdavs://")) {
		result.http_proto = "https";
		result.host = url.substr(10);
	} else if (StringUtil::StartsWith(url, "https://")) {
		result.http_proto = "https";
		result.host = url.substr(8);
	} else if (StringUtil::StartsWith(url, "http://")) {
		result.http_proto = "http";
		result.host = url.substr(7);
	} else {
		throw IOException("Invalid WebDAV URL: %s", url);
	}

	// Split host and path
	auto slash_pos = result.host.find('/');
	if (slash_pos != string::npos) {
		result.path = result.host.substr(slash_pos);
		result.host = result.host.substr(0, slash_pos);
	} else {
		result.path = "/";
	}

	return result;
}

string WebDAVFileSystem::Base64Encode(const string &input) {
	const string base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	string result;
	int val = 0;
	int valb = -6;

	for (unsigned char c : input) {
		val = (val << 8) + c;
		valb += 8;
		while (valb >= 0) {
			result.push_back(base64_chars[(val >> valb) & 0x3F]);
			valb -= 6;
		}
	}

	if (valb > -6) {
		result.push_back(base64_chars[((val << 8) >> (valb + 8)) & 0x3F]);
	}

	while (result.size() % 4) {
		result.push_back('=');
	}

	return result;
}

// Custom HTTP request using HTTP client infrastructure
duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::CustomRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                                 const string &method, char *buffer_in,
                                                                 idx_t buffer_in_len) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();

	WEBDAV_DEBUG_LOG("[WebDAV] CustomRequest called: method=%s, url=%s\n", method.c_str(), url.c_str());

	// Store the method in extra headers as a hint for custom processing
	auto &http_params = wfh.http_params;
	auto original_extra_headers = http_params.extra_headers;
	http_params.extra_headers["X-DuckDB-HTTP-Method"] = method;

	WEBDAV_DEBUG_LOG("[WebDAV] CustomRequest: Set X-DuckDB-HTTP-Method=%s\n", method.c_str());
	WEBDAV_DEBUG_LOG("[WebDAV] CustomRequest: extra_headers size=%zu\n", http_params.extra_headers.size());

	// Get the HTTP client and call Post() directly
	WEBDAV_DEBUG_LOG("[WebDAV] CustomRequest: Getting HTTP client\n");
	fflush(stderr);
	auto client = wfh.GetClient();

	// Create PostRequestInfo and call client.Post() directly
	PostRequestInfo post_info(url, header_map, http_params, const_data_ptr_cast(buffer_in), buffer_in_len);
	WEBDAV_DEBUG_LOG("[WebDAV] CustomRequest: About to call client->Post()\n");
	fflush(stderr);
	auto result = client->Post(post_info);
	WEBDAV_DEBUG_LOG("[WebDAV] CustomRequest: Post() completed\n");
	fflush(stderr);

	// Copy the result body
	if (result) {
		result->body = std::move(post_info.buffer_out);
	}

	// Restore headers
	http_params.extra_headers = original_extra_headers;

	return result;
}

string WebDAVFileSystem::DirectPropfindRequest(const string &url, const WebDAVAuthParams &auth_params, int depth) {
	// We need a file handle to make HTTP requests through the proper infrastructure
	// Since we're being called from Glob which has an opener, we should create a temporary handle
	// For now, we'll return empty and the caller should handle creating the handle properly
	return "";
}

void WebDAVFileSystem::AddAuthHeaders(HTTPHeaders &headers, const WebDAVAuthParams &auth_params) {
	if (!auth_params.username.empty() || !auth_params.password.empty()) {
		string credentials = auth_params.username + ":" + auth_params.password;
		string encoded = Base64Encode(credentials);
		headers["Authorization"] = "Basic " + encoded;
		WEBDAV_DEBUG_LOG("[WebDAV] AddAuthHeaders: Added Authorization header for user %s\n",
		                 auth_params.username.c_str());
	} else {
		WEBDAV_DEBUG_LOG("[WebDAV] AddAuthHeaders: NO auth credentials available!\n");
	}
}

string WebDAVFileSystem::GetName() const {
	return "WebDAVFileSystem";
}

bool WebDAVFileSystem::IsWebDAVUrl(const string &url) {
	// Check for storagebox:// protocol (Hetzner Storage Box shorthand)
	if (StringUtil::StartsWith(url, "storagebox://")) {
		return true;
	}
	// Check for explicit WebDAV protocol
	if (StringUtil::StartsWith(url, "webdav://") || StringUtil::StartsWith(url, "webdavs://")) {
		return true;
	}
	// Check for Hetzner Storage Box URLs via HTTPS (these use WebDAV)
	// Only match HTTP/HTTPS URLs, not other protocols like ssh://
	if ((StringUtil::StartsWith(url, "https://") || StringUtil::StartsWith(url, "http://")) &&
	    url.find(".your-storagebox.de/") != string::npos) {
		return true;
	}
	return false;
}

bool WebDAVFileSystem::CanHandleFile(const string &fpath) {
	return IsWebDAVUrl(fpath);
}

duckdb::unique_ptr<HTTPFileHandle> WebDAVFileSystem::CreateHandle(const OpenFileInfo &file, FileOpenFlags flags,
                                                                  optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	static int call_count = 0;
	call_count++;
	WEBDAV_DEBUG_LOG("[WebDAV] CreateHandle #%d called for: %s, flags: read=%d write=%d create=%d overwrite=%d\n",
	                 call_count, file.path.c_str(), flags.OpenForReading(), flags.OpenForWriting(),
	                 flags.CreateFileIfNotExists(), flags.OverwriteExistingFile());

	// First, read auth params using ORIGINAL URL for secret matching
	// This is critical for proper secret scoping - secrets are scoped to storagebox:// URLs,
	// not the converted https:// URLs
	FileOpenerInfo info;
	info.file_path = file.path; // Use ORIGINAL URL (e.g., storagebox://u507042/file.parquet)
	auto auth_params = WebDAVAuthParams::ReadFrom(opener, info);

	// Parse and convert the URL for actual HTTP operations (e.g., storagebox:// -> https://)
	auto parsed_url = ParseUrl(file.path);
	string converted_url = parsed_url.GetHTTPUrl();

	// Create a modified file info with the converted URL for HTTP operations
	OpenFileInfo converted_file = file;
	converted_file.path = converted_url;

	// Always use HTTPFSCurlUtil to ensure CURL-based HTTP client for custom methods
	auto curl_util = make_shared_ptr<HTTPFSCurlUtil>();
	WEBDAV_DEBUG_LOG("[WebDAV] CreateHandle: Using http_util: %s\n", curl_util->GetName().c_str());
	fflush(stderr);

	auto params = curl_util->InitializeParameters(opener, &info);
	auto http_params_p = dynamic_cast<HTTPFSParams *>(params.get());
	if (!http_params_p) {
		throw InternalException("Failed to cast HTTP params");
	}

	return make_uniq<WebDAVFileHandle>(*this, converted_file, flags, std::move(params), auth_params, curl_util);
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::PropfindRequest(FileHandle &handle, string url,
                                                                   HTTPHeaders header_map, int depth) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	header_map["Depth"] = to_string(depth);
	header_map["Content-Type"] = "application/xml; charset=utf-8";

	// Basic PROPFIND request body
	string propfind_body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
	                       "<D:propfind xmlns:D=\"DAV:\">"
	                       "<D:prop>"
	                       "<D:resourcetype/>"
	                       "<D:getcontentlength/>"
	                       "<D:getlastmodified/>"
	                       "</D:prop>"
	                       "</D:propfind>";

	// Use CustomRequest which sets up PROPFIND properly
	return CustomRequest(handle, url, header_map, "PROPFIND", const_cast<char *>(propfind_body.c_str()),
	                     propfind_body.size());
}

/**
 * @brief Set a custom property on a WebDAV resource using PROPPATCH
 *
 * Implements RFC 4918 Section 9.2 (PROPPATCH Method) to set custom properties
 * on WebDAV resources. This allows storing metadata alongside files.
 *
 * @param handle File handle for authentication and client access
 * @param url Target resource URL
 * @param header_map Additional HTTP headers
 * @param property_name Name of the property to set (without namespace prefix)
 * @param property_value Value to set for the property
 * @return HTTPResponse containing the server's response
 */
duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::ProppatchRequest(FileHandle &handle, string url,
                                                                    HTTPHeaders header_map, const string &property_name,
                                                                    const string &property_value) {
	WEBDAV_DEBUG_LOG("[WebDAV] ProppatchRequest called for URL: %s, property: %s\n", url.c_str(),
	                 property_name.c_str());

	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	header_map["Content-Type"] = "application/xml; charset=utf-8";

	// Build PROPPATCH request body (RFC 4918 Section 9.2)
	// Sets a property in the custom namespace
	string proppatch_body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
	                        "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:C=\"http://duckdb.org/webdav/\">"
	                        "<D:set>"
	                        "<D:prop>"
	                        "<C:" +
	                        property_name + ">" + property_value + "</C:" + property_name +
	                        ">"
	                        "</D:prop>"
	                        "</D:set>"
	                        "</D:propertyupdate>";

	WEBDAV_DEBUG_LOG("[WebDAV] ProppatchRequest: Sending PROPPATCH request (body size: %zu)\n", proppatch_body.size());

	// Use CustomRequest which sets up PROPPATCH properly
	auto response = CustomRequest(handle, url, header_map, "PROPPATCH", const_cast<char *>(proppatch_body.c_str()),
	                              proppatch_body.size());

	WEBDAV_DEBUG_LOG("[WebDAV] ProppatchRequest: Got response %d\n", static_cast<int>(response->status));
	return response;
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::MkcolRequest(FileHandle &handle, string url,
                                                                HTTPHeaders header_map) {
	WEBDAV_DEBUG_LOG("[WebDAV] MkcolRequest called for URL: %s\n", url.c_str());

	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);

	WEBDAV_DEBUG_LOG("[WebDAV] MkcolRequest: Sending MKCOL request\n");

	// Use MKCOL to create directory (proper WebDAV method)
	auto response = CustomRequest(handle, url, header_map, "MKCOL", nullptr, 0);

	WEBDAV_DEBUG_LOG("[WebDAV] MkcolRequest: Got response %d\n", static_cast<int>(response->status));
	return response;
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::MoveRequest(FileHandle &handle, string source_url, string dest_url,
                                                               HTTPHeaders header_map) {
	WEBDAV_DEBUG_LOG("[WebDAV] MoveRequest called: %s -> %s\n", source_url.c_str(), dest_url.c_str());

	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);

	// Add required WebDAV MOVE headers (RFC 4918 Section 9.9)
	header_map["Destination"] = dest_url;
	header_map["Overwrite"] = "T"; // Allow overwriting destination if it exists

	WEBDAV_DEBUG_LOG("[WebDAV] MoveRequest: Sending MOVE request\n");

	// Use MOVE to rename/move the file (server-side operation)
	auto response = CustomRequest(handle, source_url, header_map, "MOVE", nullptr, 0);

	WEBDAV_DEBUG_LOG("[WebDAV] MoveRequest: Got response %d\n", static_cast<int>(response->status));
	return response;
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::HeadRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	return HTTPFileSystem::HeadRequest(handle, url, header_map);
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::GetRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	return HTTPFileSystem::GetRequest(handle, url, header_map);
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::GetRangeRequest(FileHandle &handle, string url,
                                                                   HTTPHeaders header_map, idx_t file_offset,
                                                                   char *buffer_out, idx_t buffer_out_len) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	return HTTPFileSystem::GetRangeRequest(handle, url, header_map, file_offset, buffer_out, buffer_out_len);
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::PutRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                              char *buffer_in, idx_t buffer_in_len, string params) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	return HTTPFileSystem::PutRequest(handle, url, header_map, buffer_in, buffer_in_len, params);
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::PutRequestFromFile(FileHandle &handle, string url,
                                                                      HTTPHeaders header_map, const string &file_path,
                                                                      idx_t file_size) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);

	WEBDAV_DEBUG_LOG("[WebDAV] PutRequestFromFile: Uploading from %s (%llu bytes)\n", file_path.c_str(),
	                 (unsigned long long)file_size);

	// Open the file for reading
	FILE *fp = fopen(file_path.c_str(), "rb");
	if (!fp) {
		throw IOException("Failed to open temp file %s for streaming upload", file_path);
	}

	// Get the HTTP client and set up file streaming
	auto &http_util = wfh.http_params.http_util;
	auto client = wfh.GetClient();
	SetHTTPClientUploadFile(client.get(), fp, file_size);

	// Create the PUT request
	string content_type = "application/octet-stream";
	PutRequestInfo put_request(url, header_map, wfh.http_params, nullptr, file_size, content_type);

	// Make the request with our configured client
	auto response = http_util.Request(put_request, client);

	// Store client back for reuse
	wfh.StoreClient(std::move(client));

	// Close the file after upload
	fclose(fp);

	return response;
}

duckdb::unique_ptr<HTTPResponse> WebDAVFileSystem::DeleteRequest(FileHandle &handle, string url,
                                                                 HTTPHeaders header_map) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	AddAuthHeaders(header_map, wfh.auth_params);
	return HTTPFileSystem::DeleteRequest(handle, url, header_map);
}

void WebDAVFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(filename);
	string http_url = parsed_url.GetHTTPUrl();

	FileOpenerInfo info;
	info.file_path = filename;
	auto auth_params = WebDAVAuthParams::ReadFrom(opener, info);

	// Create a temporary handle for the delete operation
	OpenFileInfo file_info;
	file_info.path = filename;
	auto handle = CreateHandle(file_info, FileOpenFlags::FILE_FLAGS_READ, opener);
	handle->Initialize(opener);

	HTTPHeaders headers;
	auto response = DeleteRequest(*handle, http_url, headers);

	if (response->status != HTTPStatusCode::OK_200 && response->status != HTTPStatusCode::NoContent_204 &&
	    response->status != HTTPStatusCode::Accepted_202) {
		throw IOException("Failed to delete file %s: HTTP %d", filename, static_cast<int>(response->status));
	}
}

void WebDAVFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	WEBDAV_DEBUG_LOG("[WebDAV] MoveFile called: %s -> %s\n", source.c_str(), target.c_str());

	// Parse both URLs
	auto source_parsed = ParseUrl(source);
	auto target_parsed = ParseUrl(target);
	string source_http_url = source_parsed.GetHTTPUrl();
	string target_http_url = target_parsed.GetHTTPUrl();

	// Create a handle for the source file to authenticate the MOVE request
	OpenFileInfo source_file;
	source_file.path = source;
	auto source_handle = CreateHandle(source_file, FileOpenFlags::FILE_FLAGS_READ, opener);
	source_handle->Initialize(opener);

	// Use WebDAV MOVE method for server-side atomic rename/move (RFC 4918 Section 9.9)
	// This is much more efficient than download + upload, especially for large files
	HTTPHeaders headers;
	auto response = MoveRequest(*source_handle, source_http_url, target_http_url, headers);

	// Check for successful move
	// HTTP 201 Created = destination was created
	// HTTP 204 No Content = destination was overwritten
	if (response->status != HTTPStatusCode::Created_201 && response->status != HTTPStatusCode::NoContent_204) {
		throw IOException("Failed to move file %s to %s: HTTP %d", source, target, static_cast<int>(response->status));
	}

	WEBDAV_DEBUG_LOG("[WebDAV] MoveFile: Successfully moved file (HTTP %d)\n", static_cast<int>(response->status));
}

void WebDAVFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory called for: %s\n", directory.c_str());

	auto parsed_url = ParseUrl(directory);
	string http_url = parsed_url.GetHTTPUrl();

	// Ensure the URL ends with a slash for directory creation
	if (!StringUtil::EndsWith(http_url, "/")) {
		http_url += "/";
	}

	FileOpenerInfo info;
	info.file_path = directory;
	auto auth_params = WebDAVAuthParams::ReadFrom(opener, info);

	// Create a temporary handle for the MKCOL operation
	OpenFileInfo file_info;
	file_info.path = directory;
	auto handle = CreateHandle(file_info, FileOpenFlags::FILE_FLAGS_READ, opener);

	// Try to initialize the handle - if it fails because the directory doesn't exist, that's expected
	// We're about to create it!
	try {
		handle->Initialize(opener);
		WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Handle initialized successfully\n");
	} catch (const HTTPException &e) {
		// Directory doesn't exist yet - that's fine, we're creating it
		fprintf(stderr,
		        "[WebDAV] CreateDirectory: Handle init failed (directory doesn't exist yet), proceeding with MKCOL\n");
		// Set a dummy length so the handle can be used
		handle->length = 0;
		handle->initialized = true;
	}

	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Sending MKCOL request\n");

	HTTPHeaders headers;
	auto response = MkcolRequest(*handle, http_url, headers);

	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: MKCOL returned %d\n", static_cast<int>(response->status));

	if (response->status != HTTPStatusCode::Created_201 && response->status != HTTPStatusCode::OK_200 &&
	    response->status != HTTPStatusCode::NoContent_204) {
		// Check for insufficient storage (507)
		if (response->status == HTTPStatusCode::InsufficientStorage_507) {
			WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Storage is full (507)\n");
			throw IOException("Failed to create directory %s: Storage is full. The WebDAV server has "
			                  "insufficient storage space available. Free up space or resize your storage.",
			                  directory);
		}

		// Check if parent directory doesn't exist (404 or 409 Conflict)
		if (response->status == HTTPStatusCode::NotFound_404 || response->status == HTTPStatusCode::Conflict_409) {
			WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Parent doesn't exist, creating recursively\n");
			// Extract parent directory path
			auto last_slash = directory.rfind('/');
			if (last_slash != string::npos && last_slash > 0) {
				string parent_dir = directory.substr(0, last_slash);
				// Skip protocol part (e.g., "storagebox://")
				auto protocol_end = parent_dir.find("://");
				if (protocol_end != string::npos) {
					auto first_slash_after_protocol = parent_dir.find('/', protocol_end + 3);
					if (first_slash_after_protocol != string::npos && last_slash > first_slash_after_protocol) {
						// Create parent directory recursively
						CreateDirectoryRecursive(parent_dir, opener);
						// Retry creating this directory
						response = MkcolRequest(*handle, http_url, headers);
						WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Retry MKCOL returned %d\n",
						                 static_cast<int>(response->status));
					}
				}
			}
		}

		// Check final result
		if (response->status != HTTPStatusCode::Created_201 && response->status != HTTPStatusCode::OK_200 &&
		    response->status != HTTPStatusCode::NoContent_204) {
			// Check again for insufficient storage after retry
			if (response->status == HTTPStatusCode::InsufficientStorage_507) {
				WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Storage is full (507)\n");
				throw IOException("Failed to create directory %s: Storage is full. The WebDAV server has "
				                  "insufficient storage space available. Free up space or resize your storage.",
				                  directory);
			}

			// Directory might already exist
			if (response->status != HTTPStatusCode::MethodNotAllowed_405) {
				WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: FAILED with status %d\n",
				                 static_cast<int>(response->status));
				throw IOException("Failed to create directory %s: HTTP %d", directory,
				                  static_cast<int>(response->status));
			}
			WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: Directory already exists (405)\n");
		}
	}
	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectory: SUCCESS\n");
}

void WebDAVFileSystem::CreateDirectoryRecursive(const string &directory, optional_ptr<FileOpener> opener) {
	// Use standard CreateDirectory which requires an opener
	FileOpenerInfo info;
	info.file_path = directory;
	auto auth_params = WebDAVAuthParams::ReadFrom(opener, info);

	// Parse URL to extract path components
	auto parsed_url = ParseUrl(directory);
	string path = parsed_url.path;

	// Split path into components
	vector<string> path_parts;
	string current;
	for (char c : path) {
		if (c == '/') {
			if (!current.empty()) {
				path_parts.push_back(current);
				current.clear();
			}
		} else {
			current += c;
		}
	}
	if (!current.empty()) {
		path_parts.push_back(current);
	}

	// Build up directory path incrementally
	string accumulated_path;
	string protocol_prefix;
	if (StringUtil::StartsWith(directory, "storagebox://")) {
		// Extract username from storagebox URL
		string remainder = directory.substr(13);
		auto slash_pos = remainder.find('/');
		string username = remainder.substr(0, slash_pos);
		protocol_prefix = "storagebox://" + username;
	} else if (StringUtil::StartsWith(directory, "webdav://")) {
		protocol_prefix = "webdav://" + parsed_url.host;
	} else if (StringUtil::StartsWith(directory, "webdavs://")) {
		protocol_prefix = "webdavs://" + parsed_url.host;
	} else {
		protocol_prefix = parsed_url.http_proto + "://" + parsed_url.host;
	}

	// Create each directory level
	for (const auto &part : path_parts) {
		accumulated_path += "/" + part;
		string full_path = protocol_prefix + accumulated_path;

		// Try to create this directory level
		try {
			CreateDirectory(full_path, opener);
		} catch (const IOException &e) {
			// Re-throw critical errors like insufficient storage
			string error_msg = e.what();
			if (error_msg.find("Storage is full") != string::npos ||
			    error_msg.find("insufficient storage") != string::npos) {
				throw;
			}
			// Ignore other errors - directory might already exist
			// We'll let the final write operation fail if there's a real issue
		}
	}
}

void WebDAVFileSystem::CreateDirectoryWithHandle(const string &directory, WebDAVFileHandle &handle) {
	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectoryWithHandle called for: %s\n", directory.c_str());

	// If directory is already an HTTP URL, use it directly
	string http_url;
	if (StringUtil::StartsWith(directory, "http://") || StringUtil::StartsWith(directory, "https://")) {
		http_url = directory;
	} else {
		auto parsed_url = ParseUrl(directory);
		http_url = parsed_url.GetHTTPUrl();
	}

	// Ensure the URL ends with a slash for directory creation
	if (!StringUtil::EndsWith(http_url, "/")) {
		http_url += "/";
	}

	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectoryWithHandle: Sending MKCOL to %s\n", http_url.c_str());

	HTTPHeaders headers;
	auto response = MkcolRequest(handle, http_url, headers);

	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectoryWithHandle: MKCOL returned %d\n", static_cast<int>(response->status));

	if (response->status != HTTPStatusCode::Created_201 && response->status != HTTPStatusCode::OK_200 &&
	    response->status != HTTPStatusCode::NoContent_204) {
		// Directory might already exist (405 Method Not Allowed)
		if (response->status == HTTPStatusCode::MethodNotAllowed_405) {
			return; // Directory already exists, success
		}

		// If MKCOL not supported, return (directory might not exist on this server)
		if (response->status == HTTPStatusCode::NotFound_404) {
			// Don't throw error - let the file write fail if directory truly doesn't exist
			return;
		}

		// Check for insufficient storage (507)
		if (response->status == HTTPStatusCode::InsufficientStorage_507) {
			WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectoryWithHandle: Storage is full (507)\n");
			throw IOException("Failed to create directory %s: Storage is full. The WebDAV server has "
			                  "insufficient storage space available. Free up space or resize your storage.",
			                  directory);
		}

		throw IOException("Failed to create directory %s: HTTP %d", directory, static_cast<int>(response->status));
	}
}

void WebDAVFileSystem::CreateDirectoryRecursiveWithHandle(const string &directory, WebDAVFileHandle &handle) {
	WEBDAV_DEBUG_LOG("[WebDAV] CreateDirectoryRecursiveWithHandle called for: %s\n", directory.c_str());

	// Check if this is already an HTTP(S) URL - if so, we need to reconstruct the original format
	string directory_to_use = directory;
	if (StringUtil::StartsWith(directory, "http://") || StringUtil::StartsWith(directory, "https://")) {
		// This is an HTTP URL - need to reconstruct original format
		// For now, just extract the path component and use it with CreateDirectoryWithHandle
		// which works with the handle we already have
		fprintf(
		    stderr,
		    "[WebDAV] CreateDirectoryRecursiveWithHandle: Got HTTP URL, will use CreateDirectoryWithHandle directly\n");

		// Just call CreateDirectoryWithHandle directly since we already have a handle
		CreateDirectoryWithHandle(directory_to_use, handle);
		return;
	}

	// Parse URL to extract path components
	auto parsed_url = ParseUrl(directory_to_use);
	string path = parsed_url.path;

	// Split path into components
	vector<string> path_parts;
	string current;
	for (char c : path) {
		if (c == '/') {
			if (!current.empty()) {
				path_parts.push_back(current);
				current.clear();
			}
		} else {
			current += c;
		}
	}
	if (!current.empty()) {
		path_parts.push_back(current);
	}

	// Build up directory path incrementally
	string accumulated_path;
	string protocol_prefix;
	if (StringUtil::StartsWith(directory, "storagebox://")) {
		// Extract username from storagebox URL
		string remainder = directory.substr(13);
		auto slash_pos = remainder.find('/');
		string username = remainder.substr(0, slash_pos);
		protocol_prefix = "storagebox://" + username;
	} else if (StringUtil::StartsWith(directory, "webdav://")) {
		protocol_prefix = "webdav://" + parsed_url.host;
	} else if (StringUtil::StartsWith(directory, "webdavs://")) {
		protocol_prefix = "webdavs://" + parsed_url.host;
	} else {
		protocol_prefix = parsed_url.http_proto + "://" + parsed_url.host;
	}

	// Create each directory level
	for (const auto &part : path_parts) {
		accumulated_path += "/" + part;
		string full_path = protocol_prefix + accumulated_path;

		// Try to create this directory level
		try {
			CreateDirectoryWithHandle(full_path, handle);
		} catch (const IOException &e) {
			// Ignore errors - directory might already exist
			// We'll let the final write operation fail if there's a real issue
		}
	}
}

void WebDAVFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	RemoveFile(directory, opener);
}

bool WebDAVFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	WEBDAV_DEBUG_LOG("[WebDAV] FileExists called for: %s\n", filename.c_str());

	// First check if it exists at all using the parent implementation
	try {
		if (!HTTPFileSystem::FileExists(filename, opener)) {
			WEBDAV_DEBUG_LOG("[WebDAV] FileExists: parent returned false\n");
			return false;
		}
		WEBDAV_DEBUG_LOG("[WebDAV] FileExists: parent returned true\n");
	} catch (const HTTPException &e) {
		// File doesn't exist or is inaccessible
		WEBDAV_DEBUG_LOG("[WebDAV] FileExists: parent threw HTTPException: %s\n", e.what());
		return false;
	}

	// Now check if it's actually a directory
	// WebDAV directories need a trailing slash, so we check both ways
	try {
		if (DirectoryExists(filename, opener)) {
			// It's a directory, not a file
			WEBDAV_DEBUG_LOG("[WebDAV] FileExists: DirectoryExists returned true, so NOT a file\n");
			return false;
		}
		WEBDAV_DEBUG_LOG("[WebDAV] FileExists: DirectoryExists returned false\n");
	} catch (const HTTPException &e) {
		// Ignore directory check errors - if we can't check, assume it's not a directory
		WEBDAV_DEBUG_LOG("[WebDAV] FileExists: DirectoryExists threw HTTPException\n");
	}

	// It exists and is not a directory, so it must be a file
	WEBDAV_DEBUG_LOG("[WebDAV] FileExists: Returning true (is a file)\n");
	return true;
}

bool WebDAVFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists called for: %s\n", directory.c_str());

	auto parsed_url = ParseUrl(directory);
	string http_url = parsed_url.GetHTTPUrl();

	if (!StringUtil::EndsWith(http_url, "/")) {
		http_url += "/";
	}

	FileOpenerInfo info;
	info.file_path = directory;

	// Create a temporary handle for the HEAD operation
	OpenFileInfo file_info;
	file_info.path = directory;
	auto handle = CreateHandle(file_info, FileOpenFlags::FILE_FLAGS_READ, opener);

	// Try to initialize the handle - if it fails, the directory doesn't exist
	try {
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: About to initialize handle\n");
		handle->Initialize(opener);
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: Initialize succeeded\n");
	} catch (const HTTPException &e) {
		// Directory doesn't exist or is inaccessible
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: Initialize threw HTTPException: %s\n", e.what());
		return false;
	} catch (const std::exception &e) {
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: Initialize threw std::exception: %s\n", e.what());
		return false;
	}

	// Try the HEAD request to check if the directory exists
	try {
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: About to send HEAD request\n");
		HTTPHeaders headers;
		auto response = HeadRequest(*handle, http_url, headers);
		bool exists = response->status == HTTPStatusCode::OK_200 || response->status == HTTPStatusCode::NoContent_204;
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: HEAD returned %d, exists=%d\n", static_cast<int>(response->status),
		                 exists);
		return exists;
	} catch (const HTTPException &e) {
		// Directory doesn't exist or is inaccessible
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: HEAD threw HTTPException: %s\n", e.what());
		return false;
	} catch (const std::exception &e) {
		WEBDAV_DEBUG_LOG("[WebDAV] DirectoryExists: HEAD threw std::exception: %s\n", e.what());
		return false;
	}
}

void WebDAVFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();

	WEBDAV_DEBUG_LOG("[WebDAV] Write called for: %s, bytes: %lld, location: %llu, current_offset: %llu\n",
	                 wfh.path.c_str(), nr_bytes, (unsigned long long)location, (unsigned long long)wfh.file_offset);

	// Validate that the write location matches our buffer position
	idx_t expected_location = wfh.using_temp_file ? wfh.file_offset : wfh.write_buffer.size();
	if (location != expected_location) {
		throw IOException("WebDAV does not support non-sequential writes. Expected location %llu but got %llu",
		                  (unsigned long long)expected_location, (unsigned long long)location);
	}

	const char *data = static_cast<const char *>(buffer);

	// Get streaming threshold from settings (convert MB to bytes)
	auto &http_params = dynamic_cast<HTTPFSParams &>(wfh.http_params);
	idx_t streaming_threshold = http_params.webdav_streaming_threshold_mb * 1024 * 1024;

	// Check if we should spill to temp file (buffer + new data exceeds threshold)
	if (!wfh.using_temp_file && (wfh.write_buffer.size() + nr_bytes > streaming_threshold)) {
		// Create temp file
		char temp_template[] = "/tmp/webdav_upload_XXXXXX";
		int temp_fd = mkstemp(temp_template);
		if (temp_fd < 0) {
			throw IOException("Failed to create temp file for streaming upload");
		}
		wfh.temp_file_path = temp_template;
		close(temp_fd);

		// Write existing buffer to temp file
		FILE *fp = fopen(wfh.temp_file_path.c_str(), "wb");
		if (!fp) {
			throw IOException("Failed to open temp file %s", wfh.temp_file_path);
		}
		if (!wfh.write_buffer.empty()) {
			fwrite(wfh.write_buffer.data(), 1, wfh.write_buffer.size(), fp);
		}
		fclose(fp);

		wfh.using_temp_file = true;
		wfh.write_buffer.clear(); // Free memory

		WEBDAV_DEBUG_LOG("[WebDAV] Write: Spilled to temp file %s (threshold exceeded: %llu bytes)\n",
		                 wfh.temp_file_path.c_str(), (unsigned long long)streaming_threshold);
	}

	if (wfh.using_temp_file) {
		// Append to temp file
		FILE *fp = fopen(wfh.temp_file_path.c_str(), "ab");
		if (!fp) {
			throw IOException("Failed to open temp file %s for append", wfh.temp_file_path);
		}
		fwrite(data, 1, nr_bytes, fp);
		fclose(fp);
	} else {
		// Append to memory buffer
		wfh.write_buffer.append(data, nr_bytes);
	}

	wfh.buffer_dirty = true;
	wfh.file_offset += nr_bytes;

	WEBDAV_DEBUG_LOG("[WebDAV] Write: wrote %lld bytes, total: %llu (using_temp_file=%d)\n", nr_bytes,
	                 (unsigned long long)wfh.file_offset, wfh.using_temp_file);
}

void WebDAVFileSystem::FileSync(FileHandle &handle) {
	auto &wfh = handle.Cast<WebDAVFileHandle>();
	WEBDAV_DEBUG_LOG("[WebDAV] FileSync called for: %s\n", wfh.path.c_str());
	wfh.FlushBuffer();
}

// Helper function to parse XML and extract file paths from PROPFIND response
static vector<OpenFileInfo> ParsePropfindResponse(const string &xml_response, const string &base_path) {
	vector<OpenFileInfo> result;

	// Simple XML parsing - look for <D:href> or <href> tags
	// WebDAV PROPFIND responses contain <response> elements with <href> child elements
	size_t pos = 0;
	while ((pos = xml_response.find("<D:href>", pos)) != string::npos ||
	       (pos = xml_response.find("<href>", pos)) != string::npos) {

		string tag_open = xml_response.substr(pos, 8) == "<D:href>" ? "<D:href>" : "<href>";
		string tag_close = tag_open == "<D:href>" ? "</D:href>" : "</href>";

		size_t start = pos + tag_open.length();
		size_t end = xml_response.find(tag_close, start);

		if (end == string::npos) {
			break;
		}

		string href = xml_response.substr(start, end - start);

		// URL decode the href
		string decoded_href;
		for (size_t i = 0; i < href.length(); i++) {
			if (href[i] == '%' && i + 2 < href.length()) {
				string hex = href.substr(i + 1, 2);
				char ch = static_cast<char>(std::stoi(hex, nullptr, 16));
				decoded_href += ch;
				i += 2;
			} else {
				decoded_href += href[i];
			}
		}

		// Skip the directory itself (entries ending with /)
		if (!StringUtil::EndsWith(decoded_href, "/")) {
			// Extract just the path portion (remove any host/port prefix)
			// WebDAV servers often return absolute paths like /path/to/file
			OpenFileInfo info;
			info.path = decoded_href;
			result.push_back(info);
		}

		pos = end + tag_close.length();
	}

	return result;
}

// Pattern matching helper (similar to S3)
static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end) {

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		if (!Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}

vector<OpenFileInfo> WebDAVFileSystem::Glob(const string &glob_pattern, FileOpener *opener) {
	WEBDAV_DEBUG_LOG("[WebDAV] Glob called for pattern: %s\n", glob_pattern.c_str());

	if (!opener) {
		// Without an opener, we can't authenticate, so just return the pattern
		WEBDAV_DEBUG_LOG("[WebDAV] Glob: no opener, returning pattern as-is\n");
		return {glob_pattern};
	}

	// Parse the WebDAV URL
	auto parsed_url = ParseUrl(glob_pattern);
	string path = parsed_url.path;

	// Find the first wildcard character
	auto first_wildcard_pos = path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		// No wildcards, return as-is
		return {glob_pattern};
	}

	// Extract the shared prefix path (up to the last '/' before the wildcard)
	auto last_slash_before_wildcard = path.rfind('/', first_wildcard_pos);
	string prefix_path;
	if (last_slash_before_wildcard != string::npos) {
		prefix_path = path.substr(0, last_slash_before_wildcard + 1);
	} else {
		prefix_path = "/";
	}

	// Construct the base URL for listing
	string list_url_pattern = parsed_url.http_proto + "://" + parsed_url.host + prefix_path;

	// Create a file handle for the PROPFIND request
	// Use a non-wildcard path to avoid recursive file opening
	FileOpenerInfo info;
	string non_wildcard_path;
	if (StringUtil::StartsWith(glob_pattern, "storagebox://")) {
		// Extract the username from the original pattern
		string remainder = glob_pattern.substr(13);
		auto slash_pos = remainder.find('/');
		string username = remainder.substr(0, slash_pos);
		non_wildcard_path = "storagebox://" + username + prefix_path;
	} else if (StringUtil::StartsWith(glob_pattern, "webdav://")) {
		non_wildcard_path = "webdav://" + parsed_url.host + prefix_path;
	} else if (StringUtil::StartsWith(glob_pattern, "webdavs://")) {
		non_wildcard_path = "webdavs://" + parsed_url.host + prefix_path;
	} else {
		non_wildcard_path = parsed_url.http_proto + "://" + parsed_url.host + prefix_path;
	}

	info.file_path = non_wildcard_path;

	OpenFileInfo file_info;
	file_info.path = non_wildcard_path;

	unique_ptr<WebDAVFileHandle> handle;
	try {
		auto base_handle = CreateHandle(file_info, FileOpenFlags::FILE_FLAGS_READ, opener);
		handle = unique_ptr_cast<HTTPFileHandle, WebDAVFileHandle>(std::move(base_handle));
		handle->Initialize(opener);
	} catch (HTTPException &e) {
		// If we can't create a handle, return empty result
		return {};
	}

	// Make PROPFIND request to list files
	// Note: We use depth=1 and recursively explore subdirectories
	HTTPHeaders headers;
	auto response = PropfindRequest(*handle, list_url_pattern, headers, 1);

	// WebDAV PROPFIND should return 207 Multi-Status
	// Some servers might return 200 OK
	if (!response ||
	    (response->status != HTTPStatusCode::MultiStatus_207 && response->status != HTTPStatusCode::OK_200)) {
		// PROPFIND failed, return empty result
		return {};
	}

	// Check if we got any response body
	if (response->body.empty()) {
		return {};
	}

	// Parse the XML response
	auto files = ParsePropfindResponse(response->body, prefix_path);
	string response_body = response->body;

	// For depth=1, we need to recursively explore subdirectories
	// Collect all subdirectories from the response
	vector<string> subdirs;
	size_t pos = 0;
	while ((pos = response_body.find("<D:href>", pos)) != string::npos ||
	       (pos = response_body.find("<href>", pos)) != string::npos) {

		string tag_open = response_body.substr(pos, 8) == "<D:href>" ? "<D:href>" : "<href>";
		string tag_close = tag_open == "<D:href>" ? "</D:href>" : "</href>";

		size_t start = pos + tag_open.length();
		size_t end = response_body.find(tag_close, start);

		if (end == string::npos) {
			break;
		}

		string href = response_body.substr(start, end - start);

		// URL decode
		string decoded_href;
		for (size_t i = 0; i < href.length(); i++) {
			if (href[i] == '%' && i + 2 < href.length()) {
				string hex = href.substr(i + 1, 2);
				char ch = static_cast<char>(std::stoi(hex, nullptr, 16));
				decoded_href += ch;
				i += 2;
			} else {
				decoded_href += href[i];
			}
		}

		// This is a directory if it ends with /
		if (StringUtil::EndsWith(decoded_href, "/") && decoded_href != prefix_path) {
			string subdir_url = parsed_url.http_proto + "://" + parsed_url.host + decoded_href;
			subdirs.push_back(subdir_url);
		}

		pos = end + tag_close.length();
	}

	// Recursively list subdirectories
	for (const auto &subdir_url : subdirs) {
		auto subdir_response = PropfindRequest(*handle, subdir_url, headers, 1);
		if (subdir_response && (subdir_response->status == HTTPStatusCode::MultiStatus_207 ||
		                        subdir_response->status == HTTPStatusCode::OK_200)) {
			auto subdir_files = ParsePropfindResponse(subdir_response->body, prefix_path);
			files.insert(files.end(), subdir_files.begin(), subdir_files.end());
		}
	}

	// Match the pattern against the file paths
	vector<string> pattern_splits = StringUtil::Split(path, "/");
	vector<OpenFileInfo> result;

	for (auto &file_info : files) {
		// Extract the path component from the href
		string file_path = file_info.path;

		// Remove any leading protocol/host if present
		size_t path_start = file_path.find(parsed_url.host);
		if (path_start != string::npos) {
			file_path = file_path.substr(path_start + parsed_url.host.length());
		}

		vector<string> key_splits = StringUtil::Split(file_path, "/");
		bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

		if (is_match) {
			// Reconstruct the full URL with the original protocol
			string full_url;
			if (StringUtil::StartsWith(glob_pattern, "storagebox://")) {
				// Extract the username from the original pattern
				string remainder = glob_pattern.substr(13);
				auto slash_pos = remainder.find('/');
				string username = remainder.substr(0, slash_pos);
				full_url = "storagebox://" + username + file_path;
			} else if (StringUtil::StartsWith(glob_pattern, "webdav://")) {
				full_url = "webdav://" + parsed_url.host + file_path;
			} else if (StringUtil::StartsWith(glob_pattern, "webdavs://")) {
				full_url = "webdavs://" + parsed_url.host + file_path;
			} else {
				full_url = parsed_url.http_proto + "://" + parsed_url.host + file_path;
			}

			file_info.path = full_url;
			result.push_back(file_info);
		}
	}

	return result;
}

unique_ptr<FileHandle> WebDAVFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                          optional_ptr<FileOpener> opener) {
	// Use the parent implementation directly - it properly handles missing files
	return HTTPFileSystem::OpenFileExtended(file, flags, opener);
}

bool WebDAVFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                 FileOpener *opener) {
	WEBDAV_DEBUG_LOG("[WebDAV] ListFiles called for: %s\n", directory.c_str());

	string trimmed_dir = directory;
	// Remove trailing slash if present
	if (StringUtil::EndsWith(trimmed_dir, "/")) {
		trimmed_dir = trimmed_dir.substr(0, trimmed_dir.length() - 1);
	}

	WEBDAV_DEBUG_LOG("[WebDAV] ListFiles: About to glob with pattern: %s/**\n", trimmed_dir.c_str());

	// Use Glob with ** pattern to list all files recursively
	auto glob_res = Glob(trimmed_dir + "/**", opener);

	WEBDAV_DEBUG_LOG("[WebDAV] ListFiles: Glob returned %zu results\n", glob_res.size());

	if (glob_res.empty()) {
		return false;
	}

	for (const auto &file : glob_res) {
		callback(file.path, false);
	}

	return true;
}

HTTPException WebDAVFileSystem::GetHTTPError(FileHandle &, const HTTPResponse &response, const string &url) {
	auto status_message = HTTPUtil::GetStatusMessage(response.status);
	string error = "WebDAV error on '" + url + "' (HTTP " + to_string(static_cast<int>(response.status)) + " " +
	               status_message + ")";

	// Add actionable error messages for common issues
	switch (response.status) {
	case HTTPStatusCode::Unauthorized_401:
		error += "\nAuthentication failed. Check your username and password in the WebDAV secret.";
		error += "\nVerify credentials with: CREATE SECRET ... (TYPE WEBDAV, USERNAME 'user', PASSWORD 'pass')";
		break;
	case HTTPStatusCode::NotFound_404:
		error += "\nFile or directory not found.";
		error += "\nFor write operations, the parent directory must exist. Use CREATE DIRECTORY if needed.";
		break;
	case HTTPStatusCode::Conflict_409:
		error += "\nConflict error - parent directory may not exist.";
		error += "\nCreate parent directories first with: CALL webdav_mkdir_recursive('path/to/parent/');";
		break;
	case HTTPStatusCode::InsufficientStorage_507:
		error += "\nStorage quota exceeded. Your storage box is full.";
		error += "\nFree up space by deleting files or upgrade your storage plan.";
		break;
	case HTTPStatusCode::Forbidden_403:
		error += "\nAccess forbidden. Check if:";
		error += "\n  - WebDAV is enabled on your storage box";
		error += "\n  - Your user has permission to access this path";
		error += "\n  - The path is within your allowed scope";
		break;
	case HTTPStatusCode::MethodNotAllowed_405:
		error += "\nHTTP method not allowed by server.";
		error += "\nThe server may not support this WebDAV operation.";
		break;
	default:
		// For other errors, just show the generic message
		break;
	}

	return HTTPException(response, error);
}

} // namespace duckdb
