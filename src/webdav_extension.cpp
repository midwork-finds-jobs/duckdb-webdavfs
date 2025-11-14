#define DUCKDB_EXTENSION_MAIN

#include "webdavfs_extension.hpp"
#include "webdavfs.hpp"
#include "webdav_secrets.hpp"
#include "httpfs_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register WebDAV file system
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	// Register WebDAV-specific configuration settings
	config.AddExtensionOption("webdav_debug_logging", "Enable debug logging for WebDAV operations",
	                          LogicalType::BOOLEAN, Value(false));

	config.AddExtensionOption("webdav_max_retries", "Maximum number of retries for failed WebDAV operations",
	                          LogicalType::BIGINT, Value::BIGINT(3));

	config.AddExtensionOption(
	    "webdav_streaming_threshold_mb",
	    "File size threshold in MB for streaming uploads (files larger than this are streamed from disk)",
	    LogicalType::BIGINT, Value::BIGINT(50));

	// Set up HTTP utility (CURL-based)
	// Always use HTTPFSCurlUtil for WebDAV since we need custom HTTP methods
	// Note: HTTPFSCurlUtil::GetName() returns "HTTPFS-Curl", not "HTTPFSCurlUtil"
	if (!config.http_util || config.http_util->GetName() != "HTTPFS-Curl") {
		fprintf(stderr, "[WebDAV Extension] Setting http_util to HTTPFSCurlUtil (was: %s)\n",
		        config.http_util ? config.http_util->GetName().c_str() : "null");
		fflush(stderr);
		config.http_util = make_shared_ptr<HTTPFSCurlUtil>();
	}

	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<WebDAVFileSystem>());

	// Register WebDAV secrets
	CreateWebDAVSecretFunctions::Register(loader);
}

void WebdavfsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string WebdavfsExtension::Name() {
	return "webdavfs";
}

std::string WebdavfsExtension::Version() const {
#ifdef EXT_VERSION_WEBDAV
	return EXT_VERSION_WEBDAV;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(webdavfs, loader) {
	duckdb::LoadInternal(loader);
}
}
