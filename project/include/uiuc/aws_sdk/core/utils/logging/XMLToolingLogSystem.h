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
