#include <uiuc/aws_sdk/core/utils/logging/XMLToolingLogSystem.h>

#include <cstdarg>

using namespace std;
using namespace xmltooling::logging;

using AWSLevel = Aws::Utils::Logging::LogLevel;
using XMLLevel = xmltooling::logging::Priority::PriorityLevel;


namespace UIUC {

namespace AWS_SDK {

namespace Utils {

namespace Logging {

XMLToolingLogSystem::XMLToolingLogSystem()
    : m_xmlCat(Category::getInstance("UIUC.AWS_SDK"))
{
    m_xmlLevel = m_xmlCat.getChainedPriority();
}


void XMLToolingLogSystem::Log(
    AWSLevel logLevel,
    const char* tag,
    const char* formatStr,
    ...
)
{
    Category& cat = getXMLCategory(tag);

    va_list vl;
    va_start(vl,formatStr);

    cat.logva(
        AWSToXMLLevel(logLevel),
        formatStr,
        vl
    );

    va_end(vl);
}


void XMLToolingLogSystem::LogStream(
    AWSLevel logLevel,
    const char* tag,
    const Aws::OStringStream &messageStream
)
{
    Category& cat = getXMLCategory(tag);

    cat.log(
        AWSToXMLLevel(logLevel),
        messageStream.str()
    );
}


Category& XMLToolingLogSystem::getXMLCategory(const std::string& name)
{
    if (name.empty()) {
        return m_xmlCat;
    } else {
        return Category::getInstance("UIUC.AWS_SDK." + name);
    }
}


int XMLToolingLogSystem::AWSToXMLLevel(AWSLevel logLevel) const
{
    switch (logLevel) {
        case AWSLevel::Fatal:       return XMLLevel::FATAL;
        case AWSLevel::Error:       return XMLLevel::ERROR;
        case AWSLevel::Warn:        return XMLLevel::WARN;
        case AWSLevel::Info:        return XMLLevel::INFO;
        case AWSLevel::Debug:       return XMLLevel::DEBUG;
        case AWSLevel::Trace:       return XMLLevel::DEBUG;
        case AWSLevel::Off:         return XMLLevel::NOTSET;
    }

    return XMLLevel::INFO;
}


AWSLevel XMLToolingLogSystem::XMLToAWSLevel(int priorityLevel) const
{
    switch (priorityLevel) {
        case XMLLevel::FATAL:       return AWSLevel::Fatal;
        case XMLLevel::ALERT:       return AWSLevel::Fatal;
        case XMLLevel::CRIT:        return AWSLevel::Fatal;
        case XMLLevel::ERROR:       return AWSLevel::Error;
        case XMLLevel::WARN:        return AWSLevel::Warn;
        case XMLLevel::NOTICE:      return AWSLevel::Warn;
        case XMLLevel::DEBUG:       return AWSLevel::Debug;
        case XMLLevel::NOTSET:      return AWSLevel::Off;
    }

    return AWSLevel::Info;
}

} // namespace Logging
} // namespace Utils
} // namespace AWS_SDK
} // namespace UIUC
