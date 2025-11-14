#pragma once

#include "duckdb.hpp"

namespace duckdb {
struct CreateSecretInput;
class CreateSecretFunction;
class BaseSecret;
struct SecretEntry;
class ExtensionLoader;

struct CreateWebDAVSecretFunctions {
public:
	static constexpr const char *WEBDAV_TYPE = "webdav";

	//! Register all CreateSecretFunctions
	static void Register(ExtensionLoader &loader);

protected:
	//! Internal function to create WebDAV secret
	static unique_ptr<BaseSecret> CreateSecretFunctionInternal(ClientContext &context, CreateSecretInput &input);
	//! Credential provider function
	static unique_ptr<BaseSecret> CreateWebDAVSecretFromConfig(ClientContext &context, CreateSecretInput &input);
};

} // namespace duckdb
