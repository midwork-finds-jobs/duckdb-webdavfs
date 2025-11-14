#include "httpfs_client.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// Stub implementation for HTTPFSUtil - WebDAV extension uses CURL client directly
unique_ptr<HTTPClient> HTTPFSUtil::InitializeClient(HTTPParams &http_params, const string &proto_host_port) {
	throw InternalException("HTTPFSUtil::InitializeClient stub - WebDAV extension should use HTTPFSCurlUtil");
}

} // namespace duckdb
