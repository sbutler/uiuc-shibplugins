#ifdef WIN32
# define DYNAMODB_EXPORTS __declspec(dllexport)
#else
# define DYNAMODB_EXPORTS
#endif

#include <boost/lexical_cast.hpp>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/DeleteItemRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/QueryRequest.h>
#include <aws/dynamodb/model/UpdateItemRequest.h>

#include <xmltooling/base.h>

#include <xercesc/util/XMLUniDefs.hpp>

#include <xmltooling/logging.h>
#include <xmltooling/unicode.h>
#include <xmltooling/XMLToolingConfig.h>
#include <xmltooling/util/NDC.h>
#include <xmltooling/util/StorageService.h>
#include <xmltooling/util/Threads.h>
#include <xmltooling/util/XMLHelper.h>

using namespace xmltooling;
using namespace xercesc;
using namespace boost;
using namespace std;

using namespace Aws::DynamoDB;
using namespace Aws::DynamoDB::Model;

namespace {
    static const XMLCh x_ACCESS_KEY_ID[] = UNICODE_LITERAL_11(a,c,c,e,s,s,K,e,y,I,D);
    static const XMLCh x_BATCH_SIZE[] = UNICODE_LITERAL_9(b,a,t,c,h,S,i,z,e);
    static const XMLCh x_CA_FILE[] = UNICODE_LITERAL_6(c,a,F,i,l,e);
    static const XMLCh x_CA_PATH[] = UNICODE_LITERAL_6(c,a,P,a,t,h);
    static const XMLCh x_CONNECT_TIMEOUT_MS[] = UNICODE_LITERAL_16(c,o,n,n,e,c,t,T,i,m,e,o,u,t,M,S);
    static const XMLCh x_CREDENTIALS[] = UNICODE_LITERAL_11(C,r,e,d,e,n,t,i,a,l,s);
    static const XMLCh x_ENDPOINT[] = UNICODE_LITERAL_8(e,n,d,p,o,i,n,t);
    static const XMLCh x_MAX_CONNECTIONS[] = UNICODE_LITERAL_14(m,a,x,C,o,n,n,e,c,t,i,o,n,s);
    static const XMLCh x_REGION[] = UNICODE_LITERAL_6(r,e,g,i,o,n);
    static const XMLCh x_REQUEST_TIMEOUT_MS[] = UNICODE_LITERAL_16(r,e,q,u,e,s,t,T,i,m,e,o,u,t,M,S);
    static const XMLCh x_SECRET_KEY[] = UNICODE_LITERAL_9(s,e,c,r,e,t,K,e,y);
    static const XMLCh x_SESSION_TOKEN[] = UNICODE_LITERAL_12(s,e,s,s,i,o,n,T,o,k,e,n);
    static const XMLCh x_TABLE_NAME[] = UNICODE_LITERAL_9(t,a,b,l,e,N,a,m,e);
    static const XMLCh x_VERIFY_SSL[] = UNICODE_LITERAL_9(v,e,r,i,f,y,S,S,L);

    static const char* ALLOCATION_TAG = "ShibDynamoDBStore";
    static const string CONTEXT( "Context" );
    static const string EXPIRES( "Expires" );
    static const string KEY( "Key" );
    static const string VALUE( "Value" );
    static const string VERSION( "Version" );

    static const int DEFAULT_BATCH_SIZE = 5;
    static const int DEFAULT_CONNECT_TIMEOUT_MS = 1000;
    static const int DEFAULT_REQUEST_TIMEOUT_MS = 3000;
    static const int DEFAULT_MAX_CONNECTIONS = 25;
    static const char* DEFAULT_TABLE_NAME = "shibsp_storage";
    static const bool DEFAULT_VERIFY_SSL = true;

    static const unsigned int MAX_CONTEXT_SIZE = 255;
    static const unsigned int MAX_KEY_SIZE = 255;
    static const unsigned int MAX_ITEM_SIZE = 400 * 1024;

    StorageService* DynamoDBStorageServiceFactory(const DOMElement* const &, bool);

    class DynamoDBStorageLogger : public Aws::Utils::Logging::LogSystemInterface {

    public:
        DynamoDBStorageLogger()
            : m_cat(logging::Category::getInstance("xmltooling.StorageService.DynamoDB.aws_sdk"))
        {
            m_priority = m_cat.getChainedPriority();
        }
        ~DynamoDBStorageLogger() {}

        Aws::Utils::Logging::LogLevel GetLogLevel() const {
            return priorityToLogLevel(m_priority);
        }

        void Log(
            Aws::Utils::Logging::LogLevel logLevel,
            const char* tag,
            const char* formatStr,
            ...
        ) {
            logging::Category& cat = getCategory(tag);

            va_list vl;
            va_start(vl,formatStr);

            cat.logva(
                logToPriorityLevel(logLevel),
                formatStr,
                vl
            );

            va_end(vl);
        }

        void LogStream(
            Aws::Utils::Logging::LogLevel logLevel,
            const char* tag,
            const Aws::OStringStream &messageStream
        ) {
            logging::Category& cat = getCategory(tag);

            cat.log(
                logToPriorityLevel(logLevel),
                messageStream.str()
            );
        }

    private:
        logging::Category &m_cat;
        int m_priority;

        logging::Category& getCategory(const string& name = "") {
            if (name.empty()) {
                return m_cat;
            } else {
                return logging::Category::getInstance("xmltooling.StorageService.DynamoDB.aws_sdk." + name);
            }
        }

        int logToPriorityLevel(Aws::Utils::Logging::LogLevel logLevel) const {
            switch (logLevel) {
                case Aws::Utils::Logging::LogLevel::Fatal:      return logging::Priority::PriorityLevel::FATAL;
                case Aws::Utils::Logging::LogLevel::Error:      return logging::Priority::PriorityLevel::ERROR;
                case Aws::Utils::Logging::LogLevel::Warn:       return logging::Priority::PriorityLevel::WARN;
                case Aws::Utils::Logging::LogLevel::Info:       return logging::Priority::PriorityLevel::INFO;
                case Aws::Utils::Logging::LogLevel::Debug:      return logging::Priority::PriorityLevel::DEBUG;
                case Aws::Utils::Logging::LogLevel::Trace:      return logging::Priority::PriorityLevel::DEBUG;
                case Aws::Utils::Logging::LogLevel::Off:        return logging::Priority::PriorityLevel::NOTSET;
            }

            return logging::Priority::PriorityLevel::INFO;
        }

        Aws::Utils::Logging::LogLevel priorityToLogLevel(int priorityLevel) const {
            switch (priorityLevel) {
                case logging::Priority::PriorityLevel::FATAL:   return Aws::Utils::Logging::LogLevel::Fatal;
                case logging::Priority::PriorityLevel::ALERT:   return Aws::Utils::Logging::LogLevel::Fatal;
                case logging::Priority::PriorityLevel::CRIT:    return Aws::Utils::Logging::LogLevel::Fatal;
                case logging::Priority::PriorityLevel::ERROR:   return Aws::Utils::Logging::LogLevel::Error;
                case logging::Priority::PriorityLevel::WARN:    return Aws::Utils::Logging::LogLevel::Warn;
                case logging::Priority::PriorityLevel::NOTICE:  return Aws::Utils::Logging::LogLevel::Warn;
                case logging::Priority::PriorityLevel::INFO:    return Aws::Utils::Logging::LogLevel::Info;
                case logging::Priority::PriorityLevel::DEBUG:   return Aws::Utils::Logging::LogLevel::Debug;
                case logging::Priority::PriorityLevel::NOTSET:  return Aws::Utils::Logging::LogLevel::Off;
            }

            return Aws::Utils::Logging::LogLevel::Info;
        }
    };


    class DynamoDBStorageService : public StorageService {

    public:
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
                string* pvalue = nullptr,
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
                string* pvalue = nullptr,
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

    private:
        DynamoDBStorageService(const DOMElement* e);

        template <typename T>
        T getItemN(const char* context, const char* key, const Aws::Map<Aws::String, AttributeValue> &item, const string &itemKey) const;
        const string getItemS(const char* context, const char* key, const Aws::Map<Aws::String, AttributeValue> &item, const string &itemKey) const;

        void logError(const Aws::Client::AWSError<DynamoDBErrors> &error) const;
        void logRequest(const DynamoDBRequest &request) const;

        int m_batchSize;
        Capabilities m_caps;
        std::shared_ptr<DynamoDBClient> m_client;
        logging::Category& m_log;
        const string m_tableName;

        friend StorageService* DynamoDBStorageServiceFactory(const DOMElement* const &, bool);
    };

    StorageService* DynamoDBStorageServiceFactory(const DOMElement* const & e, bool) {
        return new DynamoDBStorageService(e);
    }
};


DynamoDBStorageService::DynamoDBStorageService(const DOMElement* eRoot)
    : m_log(logging::Category::getInstance("xmltooling.StorageService.DynamoDB")),
      m_caps(
          MAX_CONTEXT_SIZE,
          MAX_KEY_SIZE,
          MAX_ITEM_SIZE
            - (CONTEXT.length() + MAX_CONTEXT_SIZE)
            - (KEY.length() + MAX_KEY_SIZE)
            - (EXPIRES.length() + 10)
            - (VERSION.length() + 10)
            - VALUE.length()
      ),
      m_tableName(XMLHelper::getAttrString(eRoot, DEFAULT_TABLE_NAME, x_TABLE_NAME)),
      m_batchSize(XMLHelper::getAttrInt(eRoot, DEFAULT_BATCH_SIZE, x_BATCH_SIZE))
{
    #ifdef _DEBUG
    NDC ndc("DynamoDBStorageService")
    #endif

    Aws::Client::ClientConfiguration clientConfig;
    {
        const string endpoint = XMLHelper::getAttrString(eRoot, "", x_ENDPOINT);
        const string region = XMLHelper::getAttrString(eRoot, "", x_REGION);

        if (endpoint.empty() && region.empty()) {
            throw XMLToolingException("DynamoDB Storage requires either endpoint or region in configuration.");
        }
        clientConfig.endpointOverride = endpoint;
        clientConfig.region = region;
        clientConfig.maxConnections = XMLHelper::getAttrInt(eRoot, DEFAULT_MAX_CONNECTIONS, x_MAX_CONNECTIONS);

        clientConfig.connectTimeoutMs = XMLHelper::getAttrInt(eRoot, DEFAULT_CONNECT_TIMEOUT_MS, x_CONNECT_TIMEOUT_MS);
        clientConfig.requestTimeoutMs = XMLHelper::getAttrInt(eRoot, DEFAULT_REQUEST_TIMEOUT_MS, x_REQUEST_TIMEOUT_MS);

        clientConfig.verifySSL = XMLHelper::getAttrBool(eRoot, DEFAULT_VERIFY_SSL, x_VERIFY_SSL);
        clientConfig.caFile = XMLHelper::getAttrString(eRoot, "", x_CA_FILE);
        clientConfig.caPath = XMLHelper::getAttrString(eRoot, "", x_CA_PATH);
    }

    const DOMElement* eCreds = XMLHelper::getFirstChildElement(eRoot, x_CREDENTIALS);
    if (eCreds) {
        const string accessKeyID = XMLHelper::getAttrString(eCreds, "", x_ACCESS_KEY_ID);
        const string secretKey = XMLHelper::getAttrString(eCreds, "", x_SECRET_KEY);
        const string sessionToken = XMLHelper::getAttrString(eCreds, "", x_SESSION_TOKEN);

        if (accessKeyID.empty())
            throw new XMLToolingException("DynamoDB Storage requires an accessKeyID in its Credentials configuration.");
        if (secretKey.empty())
            throw new XMLToolingException("DynamoDB Storage requires a secretKey in its Credentials configuration.");

        m_client = Aws::MakeShared<DynamoDBClient>(ALLOCATION_TAG,
            Aws::Auth::AWSCredentials(accessKeyID, secretKey, sessionToken),
            clientConfig
        );
    } else {
        m_client = Aws::MakeShared<DynamoDBClient>(ALLOCATION_TAG, clientConfig);
    }
}


bool DynamoDBStorageService::createString(
    const char* context,
    const char* key,
    const char* value,
    time_t expiration
)
{
    #ifdef _DEBUG
    NDC ndc("createString")
    #endif

    time_t now = time(nullptr);

    PutItemRequest request;
    request.SetTableName(m_tableName);

    request.AddItem(CONTEXT, AttributeValue(context));
    request.AddItem(KEY, AttributeValue(key));
    request.AddItem(VALUE, AttributeValue(value));

    request.AddItem(EXPIRES, AttributeValue().SetN(lexical_cast<string>(expiration)));
    request.AddItem(VERSION, AttributeValue().SetN("1"));

    // Make sure the new item doesn't already exist
    request.AddExpressionAttributeNames("#C", CONTEXT);
    request.AddExpressionAttributeNames("#K", KEY);
    request.AddExpressionAttributeNames("#E", EXPIRES);

    request.AddExpressionAttributeValues(":now", AttributeValue().SetN(lexical_cast<string>(now)));

    request.SetConditionExpression("attribute_not_exists(#C) OR (attribute_exists(#C) AND attribute_not_exists(#K)) OR (attribute_exists(#C) AND attribute_exists(#K) AND #E <= :now)");

    logRequest(request);

    auto outcome = m_client->PutItem(request);
    if (!outcome.IsSuccess()) {
        const auto &error = outcome.GetError();

        if (error.GetErrorType() == DynamoDBErrors::CONDITIONAL_CHECK_FAILED) {
            m_log.error("create string failed because conditional check failed (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
            return false;
        } else {
            m_log.error("create string failed (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
            logError(error);
            throw IOException("DynamoDB Storage create string failed.");
        }
    }

    return true;
}


int DynamoDBStorageService::readString(
    const char* context,
    const char* key,
    string* pvalue,
    time_t* pexpiration,
    int version
)
{
    #ifdef _DEBUG
    NDC ndc("readString")
    #endif

    time_t now = time(nullptr);

    if (pexpiration) {
        *pexpiration = 0;
    }
    if (pvalue) {
        pvalue->erase();
    }

    GetItemRequest request;
    request.SetTableName(m_tableName);
    request.SetConsistentRead(true);

    request.AddKey(CONTEXT, AttributeValue(context));
    request.AddKey(KEY, AttributeValue(key));

    logRequest(request);

    auto outcome = m_client->GetItem(request);
    if (!outcome.IsSuccess()) {
        m_log.error("read string failed for (table=%s; context=%s; key=%s)",
            m_tableName.c_str(),
            context,
            key
        );
        logError(outcome.GetError());
        throw IOException("DynamoDB Storage read string failed.");
    }

    const auto &item = outcome.GetResult().GetItem();
    if (item.size() == 0) {
        if (m_log.isDebugEnabled()) {
            m_log.debug("read string returned no data (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
        }
        return 0;
    }

    time_t itemExpires = getItemN<time_t>(context, key, item, EXPIRES);
    if (itemExpires && itemExpires <= now) {
        if (m_log.isDebugEnabled()) {
            m_log.debug("read string returned expired item (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
        }
        return 0;
    } else if (pexpiration) {
        *pexpiration = itemExpires;
    }

    int itemVersion = getItemN<int>(context, key, item, VERSION);
    if (version && itemVersion && itemVersion == version) {
        if (m_log.isDebugEnabled()) {
            m_log.debug("read string detected no version change (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
        }
    } else if (pvalue) {
        pvalue->append(getItemS(context, key, item, VALUE));
    }

    return itemVersion;
}


int DynamoDBStorageService::updateString(
    const char* context,
    const char* key,
    const char* value,
    time_t expiration,
    int version
)
{
    #ifdef _DEBUG
    NDC ndc("updateString")
    #endif

    time_t now = time(nullptr);

    UpdateItemRequest request;
    request.SetTableName(m_tableName);
    request.SetReturnValues(ReturnValue::UPDATED_NEW);

    request.AddKey(CONTEXT, AttributeValue(context));
    request.AddKey(KEY, AttributeValue(key));

    request.AddExpressionAttributeNames("#C", CONTEXT);
    request.AddExpressionAttributeNames("#K", KEY);
    request.AddExpressionAttributeNames("#E", EXPIRES);
    request.AddExpressionAttributeNames("#V", VERSION);
    request.AddExpressionAttributeNames("#VALUE", VALUE);

    {
        string conditionExpr = "attribute_exists(#C) AND attribute_exists(#K) AND #E > :now";
        request.AddExpressionAttributeValues(":now", AttributeValue().SetN(lexical_cast<string>(now)));

        if (version > 0) {
            conditionExpr += " AND #V = :ver";
            request.AddExpressionAttributeValues(":ver", AttributeValue().SetN(lexical_cast<string>(version)));
        }

        request.SetConditionExpression(conditionExpr);
    }

    {
        string updateExpr = "SET #VALUE = :value, #V = #V + :one";
        request.AddExpressionAttributeValues(":value", AttributeValue(value));
        request.AddExpressionAttributeValues(":one", AttributeValue().SetN("1"));

        if (expiration > 0) {
            updateExpr += ", #E = :expires";
            request.AddExpressionAttributeValues(":expires", AttributeValue().SetN(lexical_cast<string>(expiration)));
        }

        request.SetUpdateExpression(updateExpr);
    }

    logRequest(request);

    auto outcome = m_client->UpdateItem(request);
    if (!outcome.IsSuccess()) {
        const auto &error = outcome.GetError();

        if (error.GetErrorType() == DynamoDBErrors::CONDITIONAL_CHECK_FAILED) {
            m_log.info("update string failed with condition check failure (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );

            // see why the condition expression failed. Version
            // mismatch or value doesn't exist?
            int currVersion = this->readString(
                context,
                key,
                nullptr,
                nullptr,
                version
            );
            return currVersion == 0 ? 0 : -1;
        } else {
            m_log.error("update string failed (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
            logError(outcome.GetError());
            throw IOException("DynamoDB Storage update string failed.");
        }
    }

    const auto &attrs = outcome.GetResult().GetAttributes();
    if (attrs.size() == 0) {
        if (m_log.isDebugEnabled()) {
            m_log.debug("update string returned no data (table=%s; context=%s; key=%s)",
                m_tableName.c_str(),
                context,
                key
            );
        }
        return 0;
    }

    int itemVersion = getItemN<int>(context, key, attrs, VERSION);
    if (itemVersion == 0) {
        m_log.error("update string returned attributes with invalid version (table=%s; context=%s; key=%s)",
            m_tableName.c_str(),
            context,
            key
        );
        return 0;
    }

    return itemVersion;
}


bool DynamoDBStorageService::deleteString(
    const char* context,
    const char* key
)
{
    #ifdef _DEBUG
    NDC ndc("deleteString")
    #endif

    DeleteItemRequest request;
    request.SetTableName(m_tableName);

    request.AddKey(CONTEXT, AttributeValue(context));
    request.AddKey(KEY, AttributeValue(key));

    logRequest(request);

    auto outcome = m_client->DeleteItem(request);
    if (!outcome.IsSuccess()) {
        m_log.error("delete string failed (table=%s; context=%s; key=%s)",
            m_tableName.c_str(),
            context,
            key
        );
        logError(outcome.GetError());
        throw IOException("DynamoDB Storage delete string failed.");
    }

    return true;
}


void DynamoDBStorageService::updateContext(const char* context, time_t expiration)
{
    #ifdef _DEBUG
    NDC ndc("updateContext")
    #endif

    time_t now = time(nullptr);

    QueryRequest qRequest;
    qRequest.SetTableName(m_tableName);
    qRequest.SetConsistentRead(true);

    qRequest.AddExpressionAttributeNames("#C", CONTEXT);
    qRequest.AddExpressionAttributeNames("#K", KEY);
    qRequest.AddExpressionAttributeNames("#E", EXPIRES);

    qRequest.AddExpressionAttributeValues(":context", AttributeValue(context));
    qRequest.AddExpressionAttributeValues(":now", AttributeValue().SetN(lexical_cast<string>(now)));

    qRequest.SetKeyConditionExpression("#C = :context");
    qRequest.SetFilterExpression("#E < :now");

    qRequest.SetSelect(Select::SPECIFIC_ATTRIBUTES);
    qRequest.SetProjectionExpression("#K");

    do {
        logRequest(qRequest);

        auto qOutcome = m_client->Query(qRequest);
        if (!qOutcome.IsSuccess()) {
            m_log.error("update context failed (table=%s; context=%s)",
                m_tableName.c_str(),
                context
            );
            logError(qOutcome.GetError());
            throw IOException("DynamoDB Storage update context failed.");
        }

        const auto &qResult = qOutcome.GetResult();

        for (const auto &item : qResult.GetItems()) {
            Aws::Map<Aws::String, AttributeValue>::const_iterator it = item.find(KEY);
            if (it == item.cend()) {
                m_log.warn("update context got item without a key value (table=%s; context=%s)",
                    m_tableName.c_str(),
                    context
                );
                continue;
            }

            UpdateItemRequest uRequest;
            uRequest.SetTableName(m_tableName);

            uRequest.AddKey(CONTEXT, AttributeValue(context));
            uRequest.AddKey(KEY, it->second);

            uRequest.AddExpressionAttributeNames("#C", CONTEXT);
            uRequest.AddExpressionAttributeNames("#K", KEY);
            uRequest.AddExpressionAttributeNames("#E", EXPIRES);

            uRequest.AddExpressionAttributeValues(":expires", AttributeValue().SetN(lexical_cast<string>(expiration)));

            uRequest.SetConditionExpression("attribute_exists(#C) AND attribute_exists(#K)");
            uRequest.SetUpdateExpression("#E = :expires");

            auto uOutcome = m_client->UpdateItem(uRequest);
            if (!uOutcome.IsSuccess()) {
                const auto &error = uOutcome.GetError();

                if (error.GetErrorType() == DynamoDBErrors::CONDITIONAL_CHECK_FAILED) {
                    m_log.info("update context failed with condition check failure (table=%s; context=%s; key=%s)",
                        m_tableName.c_str(),
                        context,
                        it->second.GetS().c_str()
                    );
                } else {
                    m_log.error("update context failed (table=%s; context=%s; key=%s)",
                        m_tableName.c_str(),
                        context,
                        it->second.GetS().c_str()
                    );
                    logError(uOutcome.GetError());
                }
            }
        }

        qRequest.SetExclusiveStartKey(qResult.GetLastEvaluatedKey());
    } while (qRequest.GetExclusiveStartKey().size() != 0);
}


void DynamoDBStorageService::deleteContext(const char* context)
{
    #ifdef _DEBUG
    NDC ndc("deleteContext")
    #endif

    QueryRequest qRequest;
    qRequest.SetTableName(m_tableName);
    qRequest.SetConsistentRead(true);

    qRequest.AddExpressionAttributeNames("#C", CONTEXT);
    qRequest.AddExpressionAttributeNames("#K", KEY);

    qRequest.AddExpressionAttributeValues(":context", AttributeValue(context));

    qRequest.SetKeyConditionExpression("#C = :context");

    qRequest.SetSelect(Select::SPECIFIC_ATTRIBUTES);
    qRequest.SetProjectionExpression("#K");

    list<WriteRequest> writeRequests;
    do {
        logRequest(qRequest);

        auto qOutcome = m_client->Query(qRequest);
        if (!qOutcome.IsSuccess()) {
            m_log.error("delete context failed (table=%s; context=%s)",
                m_tableName.c_str(),
                context
            );
            logError(qOutcome.GetError());
            throw IOException("DynamoDB Storage delete context failed.");
        }

        const auto &qResult = qOutcome.GetResult();

        for (const auto &item : qResult.GetItems()) {
            Aws::Map<Aws::String, AttributeValue>::const_iterator it = item.find(KEY);
            if (it == item.cend()) {
                m_log.warn("update context got item without a key value (table=%s; context=%s)",
                    m_tableName.c_str(),
                    context
                );
                continue;
            }

            writeRequests.push_back(WriteRequest().WithDeleteRequest(
                DeleteRequest().AddKey(CONTEXT, AttributeValue(context)).AddKey(KEY, it->second)
            ));
        }

        qRequest.SetExclusiveStartKey(qResult.GetLastEvaluatedKey());
    } while (qRequest.GetExclusiveStartKey().size() != 0);

    Aws::Map<Aws::String, Aws::Vector<WriteRequest>> bwRequestItems;
    auto writeRequestsIt = writeRequests.cbegin();
    while (writeRequestsIt != writeRequests.cend()) {
        auto &bwRequestItemValue = bwRequestItems[m_tableName];
        for (; bwRequestItemValue.size() < m_batchSize && writeRequestsIt != writeRequests.cend(); ++writeRequestsIt) {
            bwRequestItemValue.push_back(*writeRequestsIt);
        }

        BatchWriteItemRequest bwRequest;
        bwRequest.SetRequestItems(bwRequestItems);

        logRequest(bwRequest);

        const auto &bwOutcome = m_client->BatchWriteItem(bwRequest);
        if (!bwOutcome.IsSuccess()) {
            m_log.error("delete context batch write failed (table=%s; context=%s)",
                m_tableName.c_str(),
                context
            );
            logError(bwOutcome.GetError());
            throw IOException("DynamoDB Storage delete context failed.");
        }

        const auto &bwResult = bwOutcome.GetResult();
        bwRequestItems = bwResult.GetUnprocessedItems();
    }
}


template <typename T>
T DynamoDBStorageService::getItemN(
    const char* context,
    const char* key,
    const Aws::Map<Aws::String, AttributeValue> &item,
    const string &itemKey
) const
{
    Aws::Map<Aws::String, AttributeValue>::const_iterator it = item.find(itemKey);
    if (it == item.cend()) {
        m_log.warn("item has no %s (table=%s; context=%s; key=%s)",
            itemKey.c_str(),
            m_tableName.c_str(),
            context,
            key
        );
        return 0;
    }

    const string itemValue = it->second.GetN();
    if (itemValue.empty()) {
        m_log.warn("item has %s that is not numeric (table=%s; context=%s; key=%s)",
            itemKey.c_str(),
            m_tableName.c_str(),
            context,
            key
        );
        return 0;
    }

    try {
        return lexical_cast<T>(itemValue);
    } catch (const bad_lexical_cast &) {
        m_log.warn("item has %s that cannot be cast: %s (table=%s; context=%s; key=%s)",
            itemKey.c_str(),
            itemValue.c_str(),
            m_tableName.c_str(),
            context,
            key
        );
        return 0;
    }
}

const string DynamoDBStorageService::getItemS(
    const char* context,
    const char* key,
    const Aws::Map<Aws::String, AttributeValue> &item,
    const string &itemKey
) const
{
    Aws::Map<Aws::String, AttributeValue>::const_iterator it = item.find(itemKey);
    if (it == item.cend()) {
        m_log.warn("item has no %s (table=%s; context=%s; key=%s)",
            itemKey.c_str(),
            m_tableName.c_str(),
            context,
            key
        );
        return "";
    }

    const string itemValue = it->second.GetS();
    if (itemValue.empty()) {
        m_log.warn("item has %s that is empty (table=%s; context=%s; key=%s)",
            itemKey.c_str(),
            m_tableName.c_str(),
            context,
            key
        );
    }

    return itemValue;
}


void DynamoDBStorageService::logError(const Aws::Client::AWSError<DynamoDBErrors> &error) const
{
    m_log.error("DynamoDB Error (%s): %s",
        error.GetExceptionName().c_str(),
        error.GetMessage().c_str()
    );
}

void DynamoDBStorageService::logRequest(const DynamoDBRequest &request) const
{
    if (!m_log.isDebugEnabled()) {
        return;
    }

    m_log.debug("DynamoDB Request: %s", request.SerializePayload().c_str());
}


extern "C" int DYNAMODB_EXPORTS xmltooling_extension_init(void*)
{
    Aws::SDKOptions options;

    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    options.loggingOptions.logger_create_fn = []() {
        return std::shared_ptr<Aws::Utils::Logging::LogSystemInterface>(new DynamoDBStorageLogger());
    };
    Aws::InitAPI(options);

    // Register this SS type
    XMLToolingConfig::getConfig().StorageServiceManager.registerFactory("DYNAMODB", DynamoDBStorageServiceFactory);
    return 0;
}

extern "C" void DYNAMODB_EXPORTS xmltooling_extension_term()
{
    XMLToolingConfig::getConfig().StorageServiceManager.deregisterFactory("DYNAMODB");

    Aws::SDKOptions options;
    Aws::ShutdownAPI(options);
}
