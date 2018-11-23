#pragma once
#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/DynamoDBRequest.h>
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

private:
    DynamoDBStorageService(const xercesc::DOMElement* e);

    template <typename T>
    T getItemN(const char* context, const char* key, const Item &item, const std::string &itemKey) const;
    const std::string getItemS(const char* context, const char* key, const Item &item, const std::string &itemKey) const;

    void logError(const Aws::Client::AWSError<Aws::DynamoDB::DynamoDBErrors> &error) const;
    void logRequest(const Aws::DynamoDB::DynamoDBRequest &request) const;

    int m_batchSize;
    Capabilities m_caps;
    std::shared_ptr<Aws::DynamoDB::DynamoDBClient> m_client;
    xmltooling::logging::Category& m_log;
    std::string m_tableName;

    friend xmltooling::StorageService* DynamoDBStorageServiceFactory(const xercesc::DOMElement* const &, bool);
};


} // namespace XMLTooling
} // namespace UIUC
