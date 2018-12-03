#ifdef WIN32
# define UIUC_SHIBPLUGINS_EXPORTS __declspec(dllexport)
#else
# define UIUC_SHIBPLUGINS_EXPORTS
#endif

#include <uiuc/aws_sdk/core/utils/logging/XMLToolingLogSystem.h>
#include <uiuc/xmltooling/DynamoDBStorageService.h>

#include <xmltooling/XMLToolingConfig.h>

using namespace xmltooling;
using namespace std;

static Aws::SDKOptions sdkOptions;


extern "C" int UIUC_SHIBPLUGINS_EXPORTS xmltooling_extension_init(void*)
{
    sdkOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    sdkOptions.loggingOptions.logger_create_fn = []() {
        return make_shared<UIUC::AWS_SDK::Utils::Logging::XMLToolingLogSystem>();
    };
    Aws::InitAPI(sdkOptions);

    // Register this SS type
    XMLToolingConfig::getConfig().StorageServiceManager.registerFactory("UIUC-DynamoDB", UIUC::XMLTooling::DynamoDBStorageServiceFactory);
    return 0;
}

extern "C" void UIUC_SHIBPLUGINS_EXPORTS xmltooling_extension_term()
{
    XMLToolingConfig::getConfig().StorageServiceManager.deregisterFactory("UIUC-DynamoDB");

    Aws::ShutdownAPI(sdkOptions);
}
