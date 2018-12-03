#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <boost/format.hpp>
#include <boost/program_options.hpp>
#include <fstream>
#include <xmltooling/XMLToolingConfig.h>
#include <xmltooling/util/ParserPool.h>
#include <xmltooling/util/StorageService.h>
#include <xmltooling/util/XMLHelper.h>

using namespace Aws::Utils::Json;
using namespace xmltooling;
using namespace xercesc;
using namespace boost;
using namespace std;

namespace po = boost::program_options;

string opt_arg0;
bool opt_debug = false;
string opt_library;
string opt_plugin;
string opt_config;
string opt_command;
vector<string> opt_commandArgs;

po::options_description global_descVisible("Global options");
po::options_description global_descPositional("Global positional options");


class ScopedXMLToolingConfig {
public:
    ScopedXMLToolingConfig(const char* loggingLevel, const string& libraryFileName = DYNAMODB_LIB_NAME) {
        XMLToolingConfig &c = XMLToolingConfig::getConfig();

        c.log_config(loggingLevel);
        c.init();
        c.load_library(libraryFileName.c_str());
    }

    ~ScopedXMLToolingConfig() {
        XMLToolingConfig::getConfig().term();
    }
};

class base_value_error : public runtime_error {

public:
    explicit base_value_error(
        const string& context_arg,
        const string& key_arg,
        const char* what_format
    )
        : m_context(context_arg), m_key(key_arg),
          runtime_error(str(format(what_format) % context_arg % key_arg))
    {}

    explicit base_value_error(
        const char* context_arg,
        const char* key_arg,
        const char* what_format
    )
        : m_context(context_arg), m_key(key_arg),
          runtime_error(str(format(what_format) % context_arg % key_arg))
    {}

    const string& context() const { return m_context; }
    const string& key() const { return m_key; }

private:
    string m_context;
    string m_key;
};

class options_error : public runtime_error {

public:
    options_error(bool helpDisplayed = false)
        : m_helpDisplayed(helpDisplayed), runtime_error("error parsing program options")
    {}

    bool isHelpDisplayed() const { return m_helpDisplayed; }

private:
    bool m_helpDisplayed = false;
};


void outputHelp(ostream &out = cerr)
{
    out << "Usage: " << opt_arg0 << " [global options] [subcommand] [subcommand options]" << endl;
    out << global_descVisible << endl;
}

void outputHelp(const string& subcommandUsage, const po::options_description& subcommandDesc, ostream &out = cerr)
{
    out << "Usage: " << opt_arg0 << " [global options] " << subcommandUsage << endl;
    out << subcommandDesc << endl;
    out << global_descVisible << endl;
}


JsonValue handleCreate(std::shared_ptr<StorageService> store)
{
    string opt_context;
    string opt_key;
    string opt_value;
    time_t opt_expiration = 0;

    po::options_description desc(opt_command + " options");
    desc.add_options()
        ("context", po::value<string>(&opt_context)->required(), "context name")
        ("key", po::value<string>(&opt_key)->required(), "key name")
        ("value", po::value<string>(&opt_value)->required(), "value")
        ("expiration", po::value<time_t>(&opt_expiration)->required(), "expiration time (UTC timestamp)")
    ;

    po::positional_options_description pos;
    pos.add("context", 1)
        .add("key", 1)
        .add("value", 1)
        .add("expiration", 1);

    po::variables_map vm;
    po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
        .options(desc)
        .positional(pos);
    try {
        po::store(parser.run(), vm);
        po::notify(vm);
    } catch (const std::exception &ex) {
        cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
        outputHelp(opt_command + " [context] [key] [value] [expiration]", desc);

        throw options_error(true);
    }

    bool success = false;
    if (opt_command == "createString")
        success = store->createString(
            opt_context.c_str(),
            opt_key.c_str(),
            opt_value.c_str(),
            opt_expiration
        );
    else
        success = store->createText(
            opt_context.c_str(),
            opt_key.c_str(),
            opt_value.c_str(),
            opt_expiration
        );

    return JsonValue()
        .WithString("context", opt_context).WithString("key", opt_key)
        .WithBool("result", success);
}

JsonValue handleDelete(std::shared_ptr<StorageService> store)
{
    string opt_context;
    string opt_key;

    po::options_description desc(opt_command + " options");
    desc.add_options()
        ("context", po::value<string>(&opt_context)->required(), "context name")
        ("key", po::value<string>(&opt_key)->required(), "key name")
    ;

    po::positional_options_description pos;
    pos.add("context", 1)
        .add("key", 1);

    po::variables_map vm;
    po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
        .options(desc)
        .positional(pos);
    try {
        po::store(parser.run(), vm);
        po::notify(vm);
    } catch (const std::exception &ex) {
        cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
        outputHelp(opt_command + " [context] [key]", desc);

        throw options_error(true);
    }

    bool success = false;
    if (opt_command == "deleteString")
        success = store->deleteString(
            opt_context.c_str(),
            opt_key.c_str()
        );
    else
        success = store->deleteText(
            opt_context.c_str(),
            opt_key.c_str()
        );

    return JsonValue()
        .WithString("context", opt_context)
        .WithString("key", opt_key)
        .WithBool("result", success);
}

JsonValue handleDeleteContext(std::shared_ptr<StorageService> store)
{
    string opt_context;

    po::options_description desc(opt_command + " options");
    desc.add_options()
        ("context", po::value<string>(&opt_context)->required(), "context name")
    ;

    po::positional_options_description pos;
    pos.add("context", 1);

    po::variables_map vm;
    po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
        .options(desc)
        .positional(pos);
    try {
        po::store(parser.run(), vm);
        po::notify(vm);
    } catch (const std::exception &ex) {
        cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
        outputHelp(opt_command + " [context]", desc);

        throw options_error(true);
    }

    store->deleteContext(
        opt_context.c_str()
    );

    return JsonValue()
        .WithString("context", opt_context)
        .WithBool("result", true);
}

JsonValue handleRead(std::shared_ptr<StorageService> store)
{
    string opt_context;
    string opt_key;
    bool opt_skip_value = false;
    bool opt_skip_expiration = false;
    int opt_version = 0;

    po::options_description desc(opt_command + " options");
    desc.add_options()
        ("context", po::value<string>(&opt_context)->required(), "context name")
        ("key", po::value<string>(&opt_key)->required(), "key name")
        ("version", po::value<int>(&opt_version), "read only if this version")
        ("skip-value", po::bool_switch(&opt_skip_value), "skip returning the value")
        ("skip-expiration", po::bool_switch(&opt_skip_expiration), "skip returning the expiration")
    ;

    po::positional_options_description pos;
    pos.add("context", 1)
        .add("key", 1);

    po::variables_map vm;
    po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
        .options(desc)
        .positional(pos);
    try {
        po::store(parser.run(), vm);
        po::notify(vm);
    } catch (const std::exception &ex) {
        cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
        outputHelp(opt_command + " [context] [key] [command options]", desc);

        throw options_error(true);
    }

    int version = 0;
    string value;
    time_t expiration = 0;

    if (opt_command == "readString")
        version = store->readString(
            opt_context.c_str(),
            opt_key.c_str(),
            opt_skip_value ? nullptr : &value,
            opt_skip_expiration ? nullptr : &expiration,
            opt_version
        );
    else
        version = store->readText(
            opt_context.c_str(),
            opt_key.c_str(),
            opt_skip_value ? nullptr : &value,
            opt_skip_expiration ? nullptr : &expiration,
            opt_version
        );

    JsonValue rv;

    rv.WithString("context", opt_context).WithString("key", opt_key);

    if (version == 0)
        return rv.WithBool("result", false);

    rv.WithInteger("version", version);
    if (!opt_skip_value) {
        if (opt_version) {
            if (opt_version == version) {
                rv.WithBool("version_changed", false);
            } else {
                rv.WithString("value", value);
                rv.WithBool("version_changed", true);
            }
        } else {
            rv.WithString("value", value);
        }
    }
    if (!opt_skip_expiration)
        rv.WithInteger("expiration", expiration);

    return rv.WithBool("result", true);
}

JsonValue handleUpdate(std::shared_ptr<StorageService> store)
{
    string opt_context;
    string opt_key;
    string opt_value;
    time_t opt_expiration = 0;
    int opt_version = 0;

    po::options_description desc(opt_command + " options");
    desc.add_options()
        ("context", po::value<string>(&opt_context)->required(), "context name")
        ("key", po::value<string>(&opt_key)->required(), "key name")
        ("value", po::value<string>(&opt_value)->required(), "value")
        ("expiration", po::value<time_t>(&opt_expiration), "expiration time (UTC timestamp); only updated if specified")
        ("version", po::value<int>(&opt_version), "only update if the version number matches")
    ;

    po::positional_options_description pos;
    pos.add("context", 1)
        .add("key", 1)
        .add("value", 1);

    po::variables_map vm;
    po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
        .options(desc)
        .positional(pos);
    try {
        po::store(parser.run(), vm);
        po::notify(vm);
    } catch (const std::exception &ex) {
        cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
        outputHelp(opt_command + " [context] [key] [value] [command options]", desc);

        throw options_error(true);
    }

    int version = 0;
    if (opt_command == "updateString")
        version = store->updateString(
            opt_context.c_str(),
            opt_key.c_str(),
            opt_value.c_str(),
            opt_expiration,
            opt_version
        );
    else
        version = store->updateText(
            opt_context.c_str(),
            opt_key.c_str(),
            opt_value.c_str(),
            opt_expiration,
            opt_version
        );

    JsonValue rv;

    rv.WithString("context", opt_context).WithString("key", opt_key);
    rv.WithInteger("version", version);
    rv.WithBool("result", (version > 0));
    return rv;
}

JsonValue handleUpdateContext(std::shared_ptr<StorageService> store)
{
    string opt_context;
    time_t opt_expiration = 0;

    po::options_description desc(opt_command + " options");
    desc.add_options()
        ("context", po::value<string>(&opt_context)->required(), "context name")
        ("expiration", po::value<time_t>(&opt_expiration)->required(), "expiration time (UTC timestamp)")
    ;

    po::positional_options_description pos;
    pos.add("context", 1)
        .add("expiration", 1);

    po::variables_map vm;
    po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
        .options(desc)
        .positional(pos);
    try {
        po::store(parser.run(), vm);
        po::notify(vm);
    } catch (const std::exception &ex) {
        cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
        outputHelp(opt_command + " [context] [expiration]", desc);

        throw options_error(true);
    }

    store->updateContext(
        opt_context.c_str(),
        opt_expiration
    );

    return JsonValue().WithString("context", opt_context).WithBool("result", true);
}


std::shared_ptr<StorageService> newStorageService(const string &configFileName, const string& pluginName = "DYNAMODB")
{
    ifstream configFile(configFileName);
    if (!configFile)
        throw runtime_error("Unable to open config file: " + configFileName);

    xercesc::DOMDocument* doc = XMLToolingConfig::getConfig().getParser().parse(configFile);
    XercesJanitor<xercesc::DOMDocument> docjanitor(doc);

    return std::shared_ptr<StorageService>(XMLToolingConfig::getConfig().StorageServiceManager.newPlugin(pluginName, doc->getDocumentElement(), true));
}


int main(int argc, char* argv[])
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    if (argc > 0)
        opt_arg0 = argv[ 0 ];

    global_descVisible.add_options()
        ("help,h", "show this help message")
        ("debug,d", po::bool_switch(&opt_debug), "turn on debug logging")
        ("library,l", po::value<string>(&opt_library)->default_value(DYNAMODB_LIB_NAME), "choose the name of the storage library to load")
        ("plugin,p", po::value<string>(&opt_plugin)->default_value("UIUC-DynamoDB"), "name the plugin registers with XMLTooling")
        ("config,c", po::value<string>(&opt_config)->required(), "filename to load for the storage service configuration")
    ;

    global_descPositional.add_options()
        ("command", po::value<string>(&opt_command)->required(), "storage command to run")
        ("subargs", po::value<vector<string>>(), "storage command arguments")
    ;

    {
        po::positional_options_description pos;
        pos.add("command", 1)
            .add("subargs", -1);

        po::options_description desc;
        desc.add(global_descVisible).add(global_descPositional);

        po::variables_map vm;
        po::command_line_parser parser = po::command_line_parser(argc, argv)
            .options(desc)
            .positional(pos)
            .allow_unregistered();

        try {
            auto parsed = parser.run();
            po::store(parsed, vm);
            po::notify(vm);

            if (!opt_command.empty()) {
                opt_commandArgs = po::collect_unrecognized(parsed.options, po::include_positional);
                opt_commandArgs.erase(opt_commandArgs.begin());
            }
        } catch (const std::exception &ex) {
            cerr << "Exception parsing arguments: " << ex.what() << endl << endl;
            outputHelp();
            return 1;
        }

        if (vm.count("help") || opt_command.empty()) {
            outputHelp();
            return 1;
        }
    }

    ScopedXMLToolingConfig scopedConfig(
        opt_debug ? "DEBUG" : "WARN",
        opt_library
    );

    try {
        std::shared_ptr<StorageService> store = newStorageService(opt_config, opt_plugin);
        JsonValue rv;

        if (opt_command == "readString" || opt_command == "readText") {
            rv = handleRead(store);
        } else if (opt_command == "createString" || opt_command == "createText") {
            rv = handleCreate(store);
        } else if (opt_command == "deleteString" || opt_command == "deleteText") {
            rv = handleDelete(store);
        } else if (opt_command == "updateString" || opt_command == "updateText") {
            rv = handleUpdate(store);
        } else if (opt_command == "updateContext") {
            rv = handleUpdateContext(store);
        } else if (opt_command == "deleteContext") {
            rv = handleDeleteContext(store);
        } else {
            throw runtime_error("unknown command: " + opt_command);
        }

        cout << rv.View().WriteReadable() << endl;
    } catch (const options_error &optsEx) {
        if (!optsEx.isHelpDisplayed()) {
            cerr << "Exception: " << optsEx.what() << endl << endl;
            outputHelp();
        }

        return 1;
    } catch (const std::exception &ex) {
        cerr << "Exception: " << ex.what() << endl;
        return 1;
    }
}
