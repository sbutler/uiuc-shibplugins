#include <fstream>

#include <boost/program_options.hpp>

#include <xmltooling/XMLToolingConfig.h>
#include <xmltooling/util/ParserPool.h>
#include <xmltooling/util/StorageService.h>
#include <xmltooling/util/XMLHelper.h>

using namespace xmltooling;
using namespace xercesc;
using namespace boost;
using namespace std;

namespace po = boost::program_options;

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
    bool opt_debug;
    string opt_library;
    string opt_plugin;
    string opt_config;
    string opt_command;
    vector<string> opt_commandArgs;

    {
        po::options_description visible_desc("Tool to allow you to interact with the XMLTool Storage Service");
        visible_desc.add_options()
            ("help,h", "show this help message")
            ("debug,d", po::bool_switch(&opt_debug), "turn on debug logging")
            ("library,l", po::value<string>(&opt_library)->default_value(DYNAMODB_LIB_NAME), "choose the name of the storage library to load")
            ("plugin,p", po::value<string>(&opt_plugin)->default_value("DYNAMODB"), "name the plugin registers with XMLTooling")
            ("config,c", po::value<string>(&opt_config)->required(), "filename to load for the storage service configuration")
        ;

        po::options_description positional_desc;
        positional_desc.add_options()
            ("command", po::value<string>(&opt_command)->required(), "storage command to run")
            ("subargs", po::value<vector<string>>(), "storage command arguments")
        ;
        po::positional_options_description pos;
        pos.add("command", 1)
            .add("subargs", -1);

        po::options_description desc;
        desc.add(visible_desc).add(positional_desc);

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
            cerr << "Exception parsing arguments: " << ex.what() << endl;
            cout << visible_desc << endl;
            return 1;
        }

        if (vm.count("help") || opt_command.empty()) {
            cout << visible_desc << endl;
            return 1;
        }
    }

    ScopedXMLToolingConfig scopedConfig(
        opt_debug ? "DEBUG" : "WARN",
        opt_library
    );

    try {
        auto store = newStorageService(opt_config, opt_plugin);

        if (opt_command == "readString" || opt_command == "readText") {
            string opt_context;
            string opt_key;
            bool opt_value = false;
            bool opt_expiration = false;
            int opt_version = 0;

            po::options_description desc(opt_command + " sub command options");
            desc.add_options()
                ("context", po::value<string>(&opt_context)->required(), "context name")
                ("key", po::value<string>(&opt_key)->required(), "key name")
                ("value", po::bool_switch(&opt_value), "return the value")
                ("expiration", po::bool_switch(&opt_expiration), "return the expiration")
                ("version", po::value<int>(&opt_version), "read only if this version")
            ;

            po::positional_options_description pos;
            pos.add("context", 1)
                .add("key", 2);

            po::variables_map vm;
            po::command_line_parser parser = po::command_line_parser(opt_commandArgs)
                .options(desc)
                .positional(pos);
            try {
                po::store(parser.run(), vm);
                po::notify(vm);
            } catch (const std::exception &ex) {
                cerr << "Exception parsing arguments: " << ex.what() << endl;
                cerr << desc << endl;
                return 1;
            }

            int version;
            string value;
            time_t expiration;

            if (opt_command == "readString")
                version = store->readString(
                    opt_context.c_str(),
                    opt_key.c_str(),
                    opt_value ? &value : nullptr,
                    opt_expiration ? &expiration : nullptr,
                    opt_version
                );
            else
                version = store->readText(
                    opt_context.c_str(),
                    opt_key.c_str(),
                    opt_value ? &value : nullptr,
                    opt_expiration ? &expiration : nullptr,
                    opt_version
                );

            cout << "Version: " << version << endl;
            if (opt_value)
                cout << "Value: " << value << endl;
            if (opt_expiration)
                cout << "Expiration: " << expiration << endl;
        }
    } catch (const std::exception &ex) {
        cerr << "caught exception: " << ex.what() << endl;
    }
}
