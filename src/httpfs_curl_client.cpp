#include "httpfs_client.hpp"
#include "http_state.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT

#include <curl/curl.h>
#include <sys/stat.h>
#include <thread>
#include <chrono>
#include "duckdb/common/exception/http_exception.hpp"

#ifndef EMSCRIPTEN
#include "httpfs_curl_client.hpp"
#endif

namespace duckdb {

// Global debug logging flag (set per-client during initialization)
// This is a workaround since the debug macro is used in places without easy access to HTTPParams
static thread_local bool g_webdav_debug_enabled = false;

// Debug logging macro - uses thread-local flag set during client initialization
#define WEBDAV_DEBUG_LOG(...)                                                                                          \
	do {                                                                                                               \
		if (g_webdav_debug_enabled) {                                                                                  \
			fprintf(stderr, __VA_ARGS__);                                                                              \
			fflush(stderr);                                                                                            \
		}                                                                                                              \
	} while (0)

/**
 * @brief Check if a curl error code represents a retryable transient failure
 *
 * Determines whether a curl error is likely transient and worth retrying. Includes
 * connection errors, timeouts, and network transmission errors.
 *
 * @param res The curl result code from a failed request
 * @return true if the error is retryable, false otherwise
 */
static inline bool IsRetryableCurlError(CURLcode res) {
	switch (res) {
	case CURLE_COULDNT_CONNECT:
	case CURLE_COULDNT_RESOLVE_HOST:
	case CURLE_COULDNT_RESOLVE_PROXY:
	case CURLE_OPERATION_TIMEDOUT:
	case CURLE_SEND_ERROR:
	case CURLE_RECV_ERROR:
	case CURLE_PARTIAL_FILE:
	case CURLE_GOT_NOTHING:
		return true;
	default:
		return false;
	}
}

/**
 * @brief Check if an HTTP status code represents a retryable server error
 *
 * Determines whether an HTTP status code indicates a transient server issue
 * that may succeed on retry. Includes rate limiting and temporary server errors.
 *
 * @param status The HTTP status code from the response
 * @return true if the status indicates a retryable error, false otherwise
 */
static inline bool IsRetryableHTTPStatus(uint16_t status) {
	// Retry on:
	// - 429 Too Many Requests
	// - 500 Internal Server Error
	// - 502 Bad Gateway
	// - 503 Service Unavailable
	// - 504 Gateway Timeout
	return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
}

/**
 * @brief Sleep for an exponentially increasing duration before retry
 *
 * Implements exponential backoff with a maximum delay cap. The delay doubles
 * with each attempt: 100ms, 200ms, 400ms, 800ms, up to a maximum of 5 seconds.
 *
 * @param attempt The retry attempt number (0-based)
 */
static inline void ExponentialBackoff(int attempt) {
	// Exponential backoff: 100ms, 200ms, 400ms, 800ms, ...
	int delay_ms = 100 * (1 << attempt); // 100 * 2^attempt
	// Cap at 5 seconds
	if (delay_ms > 5000) {
		delay_ms = 5000;
	}
	WEBDAV_DEBUG_LOG("[CURL RETRY] Waiting %d ms before retry attempt %d\n", delay_ms, attempt + 1);
	std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
}

// we statically compile in libcurl, which means the cert file location of the build machine is the
// place curl will look. But not every distro has this file in the same location, so we search a
// number of common locations and use the first one we find.
static std::string certFileLocations[] = {
    // Arch, Debian-based, Gentoo
    "/etc/ssl/certs/ca-certificates.crt",
    // RedHat 7 based
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    // Redhat 6 based
    "/etc/pki/tls/certs/ca-bundle.crt",
    // OpenSUSE
    "/etc/ssl/ca-bundle.pem",
    // Alpine
    "/etc/ssl/cert.pem"};

//! Grab the first path that exists, from a list of well-known locations
static std::string SelectCURLCertPath() {
	for (std::string &caFile : certFileLocations) {
		struct stat buf;
		if (stat(caFile.c_str(), &buf) == 0) {
			return caFile;
		}
	}
	return std::string();
}

static std::string cert_path = SelectCURLCertPath();

struct RequestInfo {
	string url = "";
	string body = "";
	uint16_t response_code = 0;
	std::vector<HTTPHeaders> header_collection;
	// For custom HTTP methods with body
	string read_buffer = "";
	size_t read_position = 0;
	// For streaming uploads from file
	FILE *upload_file = nullptr;
	size_t upload_file_size = 0;
	// For upload progress tracking
	size_t bytes_uploaded = 0;
	std::chrono::steady_clock::time_point upload_start_time;
	std::chrono::steady_clock::time_point last_progress_time;
	int last_progress_percent = -1;
};

static size_t RequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	size_t totalSize = size * nmemb;
	std::string *str = static_cast<std::string *>(userp);
	str->append(static_cast<char *>(contents), totalSize);
	return totalSize;
}

static size_t ReadCallbackCustom(char *buffer, size_t size, size_t nitems, void *userp) {
	RequestInfo *info = static_cast<RequestInfo *>(userp);
	size_t max_bytes = size * nitems;
	size_t remaining = info->read_buffer.size() - info->read_position;
	size_t to_copy = (max_bytes < remaining) ? max_bytes : remaining;

	if (to_copy > 0) {
		memcpy(buffer, info->read_buffer.data() + info->read_position, to_copy);
		info->read_position += to_copy;
	}

	WEBDAV_DEBUG_LOG("[CURL ReadCallback] Sending %zu bytes (position=%zu, total=%zu)\n", to_copy, info->read_position,
	                 info->read_buffer.size());
	fflush(stderr);

	return to_copy;
}

static size_t ReadCallbackFile(char *buffer, size_t size, size_t nitems, void *userp) {
	RequestInfo *info = static_cast<RequestInfo *>(userp);
	size_t max_bytes = size * nitems;

	if (!info->upload_file) {
		return 0; // EOF
	}

	size_t bytes_read = fread(buffer, 1, max_bytes, info->upload_file);

	// Track upload progress
	if (bytes_read > 0 && info->upload_file_size > 0) {
		info->bytes_uploaded += bytes_read;
		auto now = std::chrono::steady_clock::now();

		// Initialize timing on first read
		if (info->bytes_uploaded == bytes_read) {
			info->upload_start_time = now;
			info->last_progress_time = now;
		}

		// Calculate progress percentage
		int progress_percent = (int)((info->bytes_uploaded * 100) / info->upload_file_size);

		// Report progress every 5% or every 2 seconds
		auto time_since_last = std::chrono::duration_cast<std::chrono::seconds>(now - info->last_progress_time).count();
		if (progress_percent != info->last_progress_percent && (progress_percent % 5 == 0 || time_since_last >= 2)) {
			auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - info->upload_start_time).count();
			if (elapsed > 0) {
				double speed_mbps = (info->bytes_uploaded / (1024.0 * 1024.0)) / elapsed;
				fprintf(stderr, "[WebDAV Upload Progress] %d%% (%zu/%zu MB) - %.2f MB/s\n", progress_percent,
				        info->bytes_uploaded / (1024 * 1024), info->upload_file_size / (1024 * 1024), speed_mbps);
				fflush(stderr);
			} else {
				fprintf(stderr, "[WebDAV Upload Progress] %d%% (%zu/%zu MB)\n", progress_percent,
				        info->bytes_uploaded / (1024 * 1024), info->upload_file_size / (1024 * 1024));
				fflush(stderr);
			}
			info->last_progress_percent = progress_percent;
			info->last_progress_time = now;
		}
	}

	return bytes_read; // Return 0 on EOF or error
}

static size_t RequestHeaderCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	size_t totalSize = size * nmemb;
	std::string header(static_cast<char *>(contents), totalSize);
	HeaderCollector *header_collection = static_cast<HeaderCollector *>(userp);

	// Trim trailing \r\n
	if (!header.empty() && header.back() == '\n') {
		header.pop_back();
		if (!header.empty() && header.back() == '\r') {
			header.pop_back();
		}
	}

	// If header starts with HTTP/... curl has followed a redirect and we have a new Header,
	// so we push back a new header_collection and store headers from the redirect there.
	if (header.rfind("HTTP/", 0) == 0) {
		header_collection->header_collection.push_back(HTTPHeaders());
		header_collection->header_collection.back().Insert("__RESPONSE_STATUS__", header);
	}

	size_t colonPos = header.find(':');

	if (colonPos != std::string::npos) {
		// Split the string into two parts
		std::string part1 = header.substr(0, colonPos);
		std::string part2 = header.substr(colonPos + 1);
		if (part2.at(0) == ' ') {
			part2.erase(0, 1);
		}

		header_collection->header_collection.back().Insert(part1, part2);
	}
	// TODO: log headers that don't follow the header format

	return totalSize;
}

CURLHandle::CURLHandle(const string &token, const string &cert_path) {
	curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize curl");
	}
	if (!token.empty()) {
		curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
	}
	if (!cert_path.empty()) {
		curl_easy_setopt(curl, CURLOPT_CAINFO, cert_path.c_str());
	}
}

CURLHandle::~CURLHandle() {
	curl_easy_cleanup(curl);
}

static idx_t httpfs_client_count = 0;

class HTTPFSCurlClient : public HTTPClient {
public:
	HTTPFSCurlClient(HTTPFSParams &http_params, const string &proto_host_port) {
		// Set WebDAV-specific settings from http_params
		g_webdav_debug_enabled = http_params.webdav_debug_logging;
		max_retries = static_cast<int>(http_params.webdav_max_retries);

		WEBDAV_DEBUG_LOG("[CURL CLIENT] HTTPFSCurlClient constructor called for proto_host_port=%s\n",
		                 proto_host_port.c_str());
		auto bearer_token = "";
		if (!http_params.bearer_token.empty()) {
			bearer_token = http_params.bearer_token.c_str();
		}
		state = http_params.state;

		// call curl_global_init if not already done by another HTTPFS Client
		InitCurlGlobal();

		curl = make_uniq<CURLHandle>(bearer_token, SelectCURLCertPath());
		request_info = make_uniq<RequestInfo>();

		// set curl options
		// follow redirects
		curl_easy_setopt(*curl, CURLOPT_FOLLOWLOCATION, 1L);

		// Curl re-uses connections by default
		if (!http_params.keep_alive) {
			curl_easy_setopt(*curl, CURLOPT_FORBID_REUSE, 1L);
		} else {
			// Enable TCP keep-alive to prevent idle connections from timing out
			// This helps maintain persistent connections for better performance
			curl_easy_setopt(*curl, CURLOPT_TCP_KEEPALIVE, 1L);

			// Wait 60 seconds before sending first keep-alive probe
			// This prevents unnecessary keep-alive traffic for short-lived connections
			curl_easy_setopt(*curl, CURLOPT_TCP_KEEPIDLE, 60L);

			// Send keep-alive probes every 60 seconds
			// If the connection is idle, this ensures we detect disconnections quickly
			curl_easy_setopt(*curl, CURLOPT_TCP_KEEPINTVL, 60L);

			// Set connection cache size to allow more concurrent connections
			// Default is 5, we increase to 10 for better parallelism
			curl_easy_setopt(*curl, CURLOPT_MAXCONNECTS, 10L);

			WEBDAV_DEBUG_LOG("[CURL] TCP keep-alive enabled: idle=60s, interval=60s, max_connections=10\n");
		}

		if (http_params.enable_curl_server_cert_verification) {
			curl_easy_setopt(*curl, CURLOPT_SSL_VERIFYPEER, 1L); // Verify the cert
			curl_easy_setopt(*curl, CURLOPT_SSL_VERIFYHOST, 2L); // Verify that the cert matches the hostname
		} else {
			curl_easy_setopt(*curl, CURLOPT_SSL_VERIFYPEER, 0L); // Override default, don't verify the cert
			curl_easy_setopt(*curl, CURLOPT_SSL_VERIFYHOST,
			                 0L); // Override default, don't verify that the cert matches the hostname
		}

		// set read timeout
		curl_easy_setopt(*curl, CURLOPT_TIMEOUT, http_params.timeout);
		// set connection timeout
		curl_easy_setopt(*curl, CURLOPT_CONNECTTIMEOUT, http_params.timeout);
		// Enable automatic compression/decompression for all supported encodings (gzip, deflate, br, zstd)
		// Empty string tells curl to use all encodings it supports and decompress automatically
		// This can significantly reduce bandwidth usage for text-based responses (PROPFIND XML, etc.)
		curl_easy_setopt(*curl, CURLOPT_ACCEPT_ENCODING, "");
		// follow redirects
		curl_easy_setopt(*curl, CURLOPT_FOLLOWLOCATION, 1L);

		// define the header callback
		curl_easy_setopt(*curl, CURLOPT_HEADERFUNCTION, RequestHeaderCallback);
		curl_easy_setopt(*curl, CURLOPT_HEADERDATA, &request_info->header_collection);
		// define the write data callback (for get requests)
		curl_easy_setopt(*curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(*curl, CURLOPT_WRITEDATA, &request_info->body);

		if (!http_params.http_proxy.empty()) {
			curl_easy_setopt(*curl, CURLOPT_PROXY,
			                 StringUtil::Format("%s:%s", http_params.http_proxy, http_params.http_proxy_port).c_str());

			if (!http_params.http_proxy_username.empty()) {
				curl_easy_setopt(*curl, CURLOPT_PROXYUSERNAME, http_params.http_proxy_username.c_str());
				curl_easy_setopt(*curl, CURLOPT_PROXYPASSWORD, http_params.http_proxy_password.c_str());
			}
		}
	}

	~HTTPFSCurlClient() {
		DestroyCurlGlobal();
	}

	void Initialize(HTTPParams &http_params) override {
		// All initialization is done in the constructor for now
		// This method is required by DuckDB 1.4.2+ HTTPClient interface
	}

	unique_ptr<HTTPResponse> Get(GetRequestInfo &info) override {
		if (state) {
			state->get_count++;
		}

		auto curl_headers = TransformHeadersCurl(info.headers);
		request_info->url = info.url;
		if (!info.params.extra_headers.empty()) {
			auto curl_params = TransformParamsCurl(info.params);
			request_info->url += "?" + curl_params;
		}

		CURLcode res;
		{
			// If the same handle served a HEAD request, we must set NOBODY back to 0L to request content again
			curl_easy_setopt(*curl, CURLOPT_NOBODY, 0L);
			curl_easy_setopt(*curl, CURLOPT_URL, request_info->url.c_str());
			curl_easy_setopt(*curl, CURLOPT_HTTPHEADER, curl_headers ? curl_headers.headers : nullptr);
			res = ExecuteWithRetry();
		}

		idx_t bytes_received = 0;
		if (!request_info->header_collection.empty() &&
		    request_info->header_collection.back().HasHeader("content-length")) {
			bytes_received = std::stoi(request_info->header_collection.back().GetHeaderValue("content-length"));
			D_ASSERT(bytes_received == request_info->body.size());
		} else {
			bytes_received = request_info->body.size();
		}
		if (state) {
			state->total_bytes_received += bytes_received;
		}

		const char *data = request_info->body.c_str();
		if (info.content_handler) {
			info.content_handler(const_data_ptr_cast(data), bytes_received);
		}

		return TransformResponseCurl(res);
	}

	unique_ptr<HTTPResponse> Put(PutRequestInfo &info) override {
		if (state) {
			state->put_count++;
			state->total_bytes_sent += info.buffer_in_len;
		}

		auto curl_headers = TransformHeadersCurl(info.headers);
		// Add content type header from info
		curl_headers.Add("Content-Type: " + info.content_type);

		// Disable "Expect: 100-continue" for large uploads to avoid HTTP 100 Continue errors
		// Some WebDAV servers (like Hetzner Storage Box) don't handle this well for large files
		constexpr idx_t LARGE_UPLOAD_THRESHOLD = 10 * 1024 * 1024; // 10 MB
		bool is_large_upload = false;
		if (info.buffer_in_len > LARGE_UPLOAD_THRESHOLD) {
			is_large_upload = true;
			curl_headers.Add("Expect:");
			WEBDAV_DEBUG_LOG("[CURL PUT] Disabled Expect: 100-continue for large upload (%llu bytes)\n",
			                 (unsigned long long)info.buffer_in_len);
		}

		// transform parameters
		request_info->url = info.url;
		if (!info.params.extra_headers.empty()) {
			auto curl_params = TransformParamsCurl(info.params);
			request_info->url += "?" + curl_params;
		}

		CURLcode res;
		{
			curl_easy_setopt(*curl, CURLOPT_URL, request_info->url.c_str());
			// Perform PUT
			curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, "PUT");

			// Check if we're streaming from a file (for large uploads)
			if (request_info->upload_file) {
				WEBDAV_DEBUG_LOG("[CURL PUT] Using streaming upload from file (%llu bytes)\n",
				                 (unsigned long long)request_info->upload_file_size);
				// Use read callback for streaming
				curl_easy_setopt(*curl, CURLOPT_UPLOAD, 1L);
				curl_easy_setopt(*curl, CURLOPT_READFUNCTION, ReadCallbackFile);
				curl_easy_setopt(*curl, CURLOPT_READDATA, request_info.get());
				curl_easy_setopt(*curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)request_info->upload_file_size);
			} else {
				// Include PUT body from memory
				curl_easy_setopt(*curl, CURLOPT_POSTFIELDS, const_char_ptr_cast(info.buffer_in));
				curl_easy_setopt(*curl, CURLOPT_POSTFIELDSIZE, info.buffer_in_len);
			}

			// For large uploads, increase the timeout to 10 minutes (600 seconds)
			// Default is 30 seconds which is too short for multi-hundred MB files
			if (is_large_upload) {
				constexpr uint64_t LARGE_UPLOAD_TIMEOUT = 600; // 10 minutes
				curl_easy_setopt(*curl, CURLOPT_TIMEOUT, LARGE_UPLOAD_TIMEOUT);
				WEBDAV_DEBUG_LOG("[CURL PUT] Set timeout to %llu seconds for large upload\n",
				                 (unsigned long long)LARGE_UPLOAD_TIMEOUT);
			}

			// Apply headers
			curl_easy_setopt(*curl, CURLOPT_HTTPHEADER, curl_headers ? curl_headers.headers : nullptr);

			res = ExecuteWithRetry();
		}

		return TransformResponseCurl(res);
	}

	unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) override {
		if (state) {
			state->head_count++;
		}

		auto curl_headers = TransformHeadersCurl(info.headers);
		request_info->url = info.url;
		// transform parameters
		if (!info.params.extra_headers.empty()) {
			auto curl_params = TransformParamsCurl(info.params);
			request_info->url += "?" + curl_params;
		}

		CURLcode res;
		{
			// Set URL
			curl_easy_setopt(*curl, CURLOPT_URL, request_info->url.c_str());

			// Perform HEAD request instead of GET
			curl_easy_setopt(*curl, CURLOPT_NOBODY, 1L);
			curl_easy_setopt(*curl, CURLOPT_HTTPGET, 0L);

			// Add headers if any
			curl_easy_setopt(*curl, CURLOPT_HTTPHEADER, curl_headers ? curl_headers.headers : nullptr);

			// Execute HEAD request
			res = ExecuteWithRetry();
		}

		return TransformResponseCurl(res);
	}

	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &info) override {
		if (state) {
			state->delete_count++;
		}

		auto curl_headers = TransformHeadersCurl(info.headers);
		// transform parameters
		request_info->url = info.url;
		if (!info.params.extra_headers.empty()) {
			auto curl_params = TransformParamsCurl(info.params);
			request_info->url += "?" + curl_params;
		}

		CURLcode res;
		{
			// Set URL
			curl_easy_setopt(*curl, CURLOPT_URL, request_info->url.c_str());

			// Set DELETE request method
			curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, "DELETE");

			// Follow redirects
			curl_easy_setopt(*curl, CURLOPT_FOLLOWLOCATION, 1L);

			// Add headers if any
			curl_easy_setopt(*curl, CURLOPT_HTTPHEADER, curl_headers ? curl_headers.headers : nullptr);

			// Execute DELETE request
			res = ExecuteWithRetry();
		}

		return TransformResponseCurl(res);
	}

	unique_ptr<HTTPResponse> Post(PostRequestInfo &info) override {
		WEBDAV_DEBUG_LOG("[CURL] Post() called: url=%s\n", info.url.c_str());
		if (state) {
			state->post_count++;
			state->total_bytes_sent += info.buffer_in_len;
		}

		auto curl_headers = TransformHeadersCurl(info.headers);
		const string content_type = "Content-Type: application/octet-stream";
		curl_headers.Add(content_type.c_str());

		// Disable "Expect: 100-continue" for large uploads to avoid HTTP 100 Continue errors
		// Some WebDAV servers (like Hetzner Storage Box) don't handle this well for large files
		constexpr idx_t LARGE_UPLOAD_THRESHOLD = 10 * 1024 * 1024; // 10 MB
		if (info.buffer_in_len > LARGE_UPLOAD_THRESHOLD) {
			curl_headers.Add("Expect:");
			WEBDAV_DEBUG_LOG("[CURL] Disabled Expect: 100-continue for large upload (%llu bytes)\n",
			                 (unsigned long long)info.buffer_in_len);
		}

		// Check if a custom HTTP method is specified (e.g., MKCOL, PROPFIND for WebDAV)
		string custom_method;
		auto method_it = info.params.extra_headers.find("X-DuckDB-HTTP-Method");
		if (method_it != info.params.extra_headers.end()) {
			custom_method = method_it->second;
		}

		// Transform parameters (excluding X-DuckDB-HTTP-Method which is a directive, not a URL param)
		request_info->url = info.url;
		if (!info.params.extra_headers.empty()) {
			auto curl_params = TransformParamsCurl(info.params);
			if (!curl_params.empty()) {
				request_info->url += "?" + curl_params;
			}
		}

		WEBDAV_DEBUG_LOG("[CURL] Final URL: %s, Custom method: %s\n", request_info->url.c_str(),
		                 custom_method.empty() ? "(none)" : custom_method.c_str());

		CURLcode res;
		{
			// Set URL
			curl_easy_setopt(*curl, CURLOPT_URL, request_info->url.c_str());

			// Handle custom methods (like WebDAV MKCOL, PROPFIND) similar to DELETE
			if (!custom_method.empty()) {
				// Set custom request method (this will NOT trigger POST mode)
				curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, custom_method.c_str());

				// If there's a request body, set it using POSTFIELDS
				// Despite the name, POSTFIELDS works with CUSTOMREQUEST
				if (info.buffer_in && info.buffer_in_len > 0) {
					curl_easy_setopt(*curl, CURLOPT_POSTFIELDS, const_char_ptr_cast(info.buffer_in));
					curl_easy_setopt(*curl, CURLOPT_POSTFIELDSIZE, info.buffer_in_len);
				}

				WEBDAV_DEBUG_LOG("[CURL] Using CUSTOMREQUEST: %s with body length: %llu\n", custom_method.c_str(),
				                 (unsigned long long)info.buffer_in_len);
			} else {
				// Regular POST
				curl_easy_setopt(*curl, CURLOPT_POST, 1L);
				if (info.buffer_in && info.buffer_in_len > 0) {
					curl_easy_setopt(*curl, CURLOPT_POSTFIELDS, const_char_ptr_cast(info.buffer_in));
					curl_easy_setopt(*curl, CURLOPT_POSTFIELDSIZE, info.buffer_in_len);
				}
			}

			// Follow redirects
			curl_easy_setopt(*curl, CURLOPT_FOLLOWLOCATION, 1L);

			// Add headers if any
			curl_easy_setopt(*curl, CURLOPT_HTTPHEADER, curl_headers ? curl_headers.headers : nullptr);

			// Execute request
			res = ExecuteWithRetry();
		}

		info.buffer_out = request_info->body;
		// Construct HTTPResponse
		return TransformResponseCurl(res);
	}

private:
	CURLRequestHeaders TransformHeadersCurl(const HTTPHeaders &header_map) {
		std::vector<std::string> headers;
		for (auto &entry : header_map) {
			const std::string new_header = entry.first + ": " + entry.second;
			headers.push_back(new_header);
		}
		CURLRequestHeaders curl_headers;
		for (auto &header : headers) {
			curl_headers.Add(header);
		}
		return curl_headers;
	}

	string TransformParamsCurl(const HTTPParams &params) {
		string result = "";
		unordered_map<string, string> escaped_params;
		bool first_param = true;
		for (auto &entry : params.extra_headers) {
			const string key = entry.first;
			// Skip X-DuckDB-HTTP-Method as it's a directive, not a URL param
			if (key == "X-DuckDB-HTTP-Method") {
				continue;
			}
			const string value = curl_easy_escape(*curl, entry.second.c_str(), 0);
			if (!first_param) {
				result += "&";
			}
			result += key + "=" + value;
			first_param = false;
		}
		return result;
	}

	void ResetRequestInfo() {
		// clear headers after transform
		request_info->header_collection.clear();
		// reset request info.
		request_info->body = "";
		request_info->url = "";
		request_info->response_code = 0;
		// reset upload file for streaming
		request_info->upload_file = nullptr;
		request_info->upload_file_size = 0;
		// reset progress tracking
		request_info->bytes_uploaded = 0;
		request_info->last_progress_percent = -1;
	}

	unique_ptr<HTTPResponse> TransformResponseCurl(CURLcode res) {
		auto status_code = HTTPStatusCode(request_info->response_code);
		auto response = make_uniq<HTTPResponse>(status_code);
		if (res != CURLcode::CURLE_OK) {
			// TODO: request error can come from HTTPS Status code toString() value.
			if (!request_info->header_collection.empty() &&
			    request_info->header_collection.back().HasHeader("__RESPONSE_STATUS__")) {
				response->request_error = request_info->header_collection.back().GetHeaderValue("__RESPONSE_STATUS__");
			} else {
				response->request_error = curl_easy_strerror(res);
			}
			return response;
		}
		response->body = request_info->body;
		response->url = request_info->url;
		if (!request_info->header_collection.empty()) {
			for (auto &header : request_info->header_collection.back()) {
				response->headers.Insert(header.first, header.second);
			}
		}
		ResetRequestInfo();
		return response;
	}

	// Execute curl request with retry logic and exponential backoff
	CURLcode ExecuteWithRetry() {
		CURLcode res;

		for (int attempt = 0; attempt <= max_retries; attempt++) {
			// Execute the request
			res = curl->Execute();

			// Get HTTP response code
			curl_easy_getinfo(*curl, CURLINFO_RESPONSE_CODE, &request_info->response_code);

			// Check if request succeeded
			if (res == CURLE_OK && !IsRetryableHTTPStatus(request_info->response_code)) {
				// Success - no retry needed
				if (attempt > 0) {
					WEBDAV_DEBUG_LOG("[CURL RETRY] Request succeeded after %d retries\n", attempt);
				}
				return res;
			}

			// Check if we should retry
			bool should_retry = false;
			string retry_reason;

			if (res != CURLE_OK && IsRetryableCurlError(res)) {
				should_retry = true;
				retry_reason = string("curl error: ") + curl_easy_strerror(res);
			} else if (IsRetryableHTTPStatus(request_info->response_code)) {
				should_retry = true;
				retry_reason = string("HTTP ") + to_string(request_info->response_code);
			}

			// If this is the last attempt, don't retry
			if (attempt >= max_retries) {
				if (attempt > 0) {
					WEBDAV_DEBUG_LOG("[CURL RETRY] Request failed after %d retries (reason: %s)\n", attempt,
					                 retry_reason.c_str());
				}
				return res;
			}

			// Retry if we should
			if (should_retry) {
				WEBDAV_DEBUG_LOG("[CURL RETRY] Request failed (reason: %s), retrying (attempt %d/%d)\n",
				                 retry_reason.c_str(), attempt + 1, max_retries);

				// Reset request info for retry (but preserve headers)
				request_info->body = "";
				request_info->response_code = 0;

				// Wait with exponential backoff before retrying
				ExponentialBackoff(attempt);
			} else {
				// Non-retryable error, return immediately
				return res;
			}
		}

		return res;
	}

private:
	unique_ptr<CURLHandle> curl;
	optional_ptr<HTTPState> state;
	unique_ptr<RequestInfo> request_info;
	int max_retries = 3; // Maximum number of retries for transient failures

	// Friend function for streaming upload support
	friend void SetHTTPClientUploadFile(HTTPClient *client, FILE *fp, size_t size);

	static std::mutex &GetRefLock() {
		static std::mutex mtx;
		return mtx;
	}

	static void InitCurlGlobal() {
		GetRefLock();
		if (httpfs_client_count == 0) {
			curl_global_init(CURL_GLOBAL_DEFAULT);
		}
		++httpfs_client_count;
	}

	static void DestroyCurlGlobal() {
		// TODO: when to call curl_global_cleanup()
		// calling it on client destruction causes SSL errors when verification is on (due to many requests).
		// GetRefLock();
		// if (httpfs_client_count == 0) {
		// 	throw InternalException("Destroying Httpfs client that did not initialize CURL");
		// }
		// --httpfs_client_count;
		// if (httpfs_client_count == 0) {
		// 	curl_global_cleanup();
		// }
	}
};

unique_ptr<HTTPClient> HTTPFSCurlUtil::InitializeClient(HTTPParams &http_params, const string &proto_host_port) {
	auto client = make_uniq<HTTPFSCurlClient>(http_params.Cast<HTTPFSParams>(), proto_host_port);
	return std::move(client);
}

unordered_map<string, string> HTTPFSCurlUtil::ParseGetParameters(const string &text) {
	unordered_map<std::string, std::string> params;

	auto pos = text.find('?');
	if (pos == std::string::npos)
		return params;

	std::string query = text.substr(pos + 1);
	std::stringstream ss(query);
	std::string item;

	while (std::getline(ss, item, '&')) {
		auto eq_pos = item.find('=');
		if (eq_pos != std::string::npos) {
			std::string key = item.substr(0, eq_pos);
			std::string value = StringUtil::URLDecode(item.substr(eq_pos + 1));
			params[key] = value;
		} else {
			params[item] = ""; // key with no value
		}
	}

	return params;
}

string HTTPFSCurlUtil::GetName() const {
	return "HTTPFS-Curl";
}

// Helper function to set upload file for streaming - callable from other modules
void SetHTTPClientUploadFile(HTTPClient *client, FILE *fp, size_t size) {
	auto *curl_client = dynamic_cast<HTTPFSCurlClient *>(client);
	if (curl_client && curl_client->request_info) {
		curl_client->request_info->upload_file = fp;
		curl_client->request_info->upload_file_size = size;
	}
}

} // namespace duckdb
