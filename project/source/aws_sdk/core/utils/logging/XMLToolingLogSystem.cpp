/* Copyright (c) 2018 University of Illinois Board of Trustees
 * All rights reserved.
 *
 * Developed by:       Technology Services
 *                     University of Illinois at Urbana-Champaign
 *                     https://techservices.illinois.edu/
 *
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal with the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimers.
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimers in the
 *   documentation and/or other materials provided with the distribution.
 * - Neither the names of Technology Services, University of Illinois at
 *   Urbana-Champaign, nor the names of its contributors may be used to
 *   endorse or promote products derived from this Software without
 *   specific prior written permission.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.
 */

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
