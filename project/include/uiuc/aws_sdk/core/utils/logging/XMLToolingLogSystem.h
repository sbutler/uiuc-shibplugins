#pragma once
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <string>
#include <xmltooling/logging.h>

namespace UIUC {

namespace AWS_SDK {

namespace Utils {

namespace Logging {

class XMLToolingLogSystem : public Aws::Utils::Logging::LogSystemInterface {

public:
    XMLToolingLogSystem();
    ~XMLToolingLogSystem() {}

    Aws::Utils::Logging::LogLevel GetLogLevel() const {
        return XMLToAWSLevel(m_xmlLevel);
    }

    void Log(
        Aws::Utils::Logging::LogLevel logLevel,
        const char* tag,
        const char* formatStr,
        ...
    );

    void LogStream(
        Aws::Utils::Logging::LogLevel logLevel,
        const char* tag,
        const Aws::OStringStream &messageStream
    );

private:
    xmltooling::logging::Category &m_xmlCat;
    int m_xmlLevel;

    xmltooling::logging::Category& getXMLCategory(const std::string& name = "");
    int AWSToXMLLevel(Aws::Utils::Logging::LogLevel logLevel) const;
    Aws::Utils::Logging::LogLevel XMLToAWSLevel(int priorityLevel) const;
};

} // namespace Logging
} // namespace Utils
} // namespace AWS_SDK
} // namespace UIUC
