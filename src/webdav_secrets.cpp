#include "webdav_secrets.hpp"
#include "duckdb/main/secret/secret.hpp"
#include <duckdb/common/string_util.hpp>

namespace duckdb {

void CreateWebDAVSecretFunctions::Register(ExtensionLoader &loader) {
	// WebDAV secret
	SecretType secret_type_webdav;
	secret_type_webdav.name = WEBDAV_TYPE;
	secret_type_webdav.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type_webdav.default_provider = "config";
	secret_type_webdav.extension = "webdav";
	loader.RegisterSecretType(secret_type_webdav);

	// WebDAV config provider
	CreateSecretFunction webdav_config_fun = {WEBDAV_TYPE, "config", CreateWebDAVSecretFromConfig};
	webdav_config_fun.named_parameters["username"] = LogicalType::VARCHAR;
	webdav_config_fun.named_parameters["password"] = LogicalType::VARCHAR;
	loader.RegisterFunction(webdav_config_fun);
}

unique_ptr<BaseSecret> CreateWebDAVSecretFunctions::CreateSecretFunctionInternal(ClientContext &context,
                                                                                 CreateSecretInput &input) {
	// Set scope to user provided scope or the default
	auto scope = input.scope;
	if (scope.empty()) {
		// Default scope includes webdav://, webdavs://, storagebox://, and Hetzner Storage Box URLs
		scope.push_back("webdav://");
		scope.push_back("webdavs://");
		scope.push_back("storagebox://"); // Hetzner Storage Box shorthand
		scope.push_back("https://");      // For Hetzner Storage Boxes and other HTTPS WebDAV servers
	}
	auto return_value = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	//! Set key value map
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "username") {
			return_value->secret_map["username"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			return_value->secret_map["password"] = named_param.second.ToString();
		}
	}

	//! Set redact keys
	return_value->redact_keys = {"password"};

	return std::move(return_value);
}

unique_ptr<BaseSecret> CreateWebDAVSecretFunctions::CreateWebDAVSecretFromConfig(ClientContext &context,
                                                                                 CreateSecretInput &input) {
	return CreateSecretFunctionInternal(context, input);
}

} // namespace duckdb
