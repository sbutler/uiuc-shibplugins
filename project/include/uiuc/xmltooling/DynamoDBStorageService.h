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
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/DynamoDBRequest.h>
#include <chrono>
#include <ctime>
#include <functional>
#include <string>
#include <xercesc/dom/DOMElement.hpp>
#include <xmltooling/base.h>
#include <xmltooling/logging.h>
#include <xmltooling/util/StorageService.h>

namespace UIUC {

namespace XMLTooling {

xmltooling::StorageService* DynamoDBStorageServiceFactory(const xercesc::DOMElement* const & e, bool);

class DynamoDBStorageService : public xmltooling::StorageService {

public:
    typedef Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> Item;

    ~DynamoDBStorageService() {}

    const Capabilities& getCapabilities() const {
        return m_caps;
    }

    bool createString(
            const char* context,
            const char* key,
            const char* value,
            time_t expiration
     );
    int readString(
            const char* context,
            const char* key,
            std::string* pvalue = nullptr,
            time_t* pexpiration = nullptr,
            int version = 0
    );
    int updateString(
            const char* context,
            const char* key,
            const char* value = nullptr,
            time_t expiration = 0,
            int version = 0
    );
    bool deleteString(
            const char* context,
            const char* key
    );


    bool createText(
            const char* context,
            const char* key,
            const char* value,
            time_t expiration
        ) {
        return createString(context, key, value, expiration);
    }
    int readText(
            const char* context,
            const char* key,
            std::string* pvalue = nullptr,
            time_t* pexpiration = nullptr,
            int version = 0
        ) {
        return readString(context, key, pvalue, pexpiration, version);
    }
    int updateText(
            const char* context,
            const char* key,
            const char* value = nullptr,
            time_t expiration = 0,
            int version = 0
        ) {
        return updateString(context, key, value, expiration, version);
    }
    bool deleteText(
            const char* context,
            const char* key
        ) {
        return deleteString(context, key);
    }

    void reap(const char* context) {}

    void updateContext(const char* context, time_t expiration);
    void deleteContext(const char* context);

    void forEachContextKey(
        const char* context,
        std::function<bool (const Aws::DynamoDB::Model::AttributeValue&)> callback
    );

    Aws::Client::ClientConfiguration getDynamoDBClientConfiguration() const { return m_clientConfig; }

private:
    DynamoDBStorageService(const xercesc::DOMElement* e);

    template <typename T>
    T getItemN(const char* context, const char* key, const Item &item, const std::string &itemKey) const;
    const std::string getItemS(const char* context, const char* key, const Item &item, const std::string &itemKey) const;

    void logError(const Aws::Client::AWSError<Aws::DynamoDB::DynamoDBErrors> &error) const;
    void logRequest(const Aws::DynamoDB::DynamoDBRequest &request) const;

    std::chrono::milliseconds m_batchBackoffMax;
    std::chrono::milliseconds m_batchBackoffScaleFactor;
    int m_batchSize;
    Capabilities m_caps;
    std::shared_ptr<Aws::DynamoDB::DynamoDBClient> m_client;
    Aws::Client::ClientConfiguration m_clientConfig;
    xmltooling::logging::Category& m_log;
    std::string m_tableName;

    friend xmltooling::StorageService* DynamoDBStorageServiceFactory(const xercesc::DOMElement* const &, bool);
};


} // namespace XMLTooling
} // namespace UIUC
