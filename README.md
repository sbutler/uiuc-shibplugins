# UIUC Shibboleth Plugins

**THIS LIBRARY IS EXPERIMENTAL. USE AT YOUR OWN RISK.**

Set of useful plugins for running Shibboleth SP3. To load the plugins
you add or modify the `<OutOfProcess>` section in shibboleth2.xml:

```xml
<OutOfProcess>
    <Extensions>
        <!-- ... other extension load elements you might have -->
        <Library path="/path/to/libuiuc-shibplugins.so" fatal="true"/>
    </Extensions>
</OutOfProcess>
```

## DynamoDB Storage Service: UIUC-DynamoDB

This is a backend storage system for shibd that uses DynamoDB. DynamoDB
provides an attractive managed storage option for a cluster of shibd
instances. It is modeled after the builtin memory, memcached, and ODBC
plugins.

The table needs to be configured as follows:

- Partition Key: Context (String)
- Sort Key: Key (String)
- Time to Live Attribute: Expires

To load this plugin create a `StorageService` element and configure it
with these attributes.

| Name              | Type    | Required? | Default | Description |
| ----------------- | ------- | --------- | ------- | ----------- |
| type              | String  | Y         |         | Specify "UIUC-DynamoDB" to use the plugin. |
| id                | XML ID  | N         |         | A unique identifier within the configuration file that labels the plugin instance so other plugins can reference it. |
| tableName         | String  | N         | shibsp_storage | Name of the DynamoDB table to use. This table must already exist and be configured as specified above. |
| batchSize         | Integer | N         | 5       | When performing batch operations, how many requests to perform at once. |
| region            | String  | Y         |         | The AWS region identifier (us-east-1, us-east-2, etc) for the DynamoDB table. Either this attribute or endpoint must be specified. |
| endpoint          | String  | Y         |         | The endpoint URL for the DynamoDB service. Either this attribute or region must be specified. |
| maxConnections    | Integer | N         | 25      | Maximum number of simultaneous connections that the client will make to DynamoDB. |
| connectTimeoutMS  | Integer | N         | 1000    | Timeout value in milliseconds to wait for a successful connection to DynamoDB. |
| requestTimeoutMS  | Integer | N         | 3000    | Timeout value in milliseconds to wait for a response when performing DynamoDB requests. |
| verifySSL         | Boolean | N         | true    | Verify the SSL certificate when connecting to DynamoDB. |
| caFile            | String  | N         |         | Path to a file of CA certificates to use for verifying the SSL connection. |
| caPath            | String  | N         |         | Path to a directory of hashed CA certificates to use for verifying the SSL connection. |

AWS credentials are searched for in the standard fashion, using
environment variables and standard configuration locations. The client
will also use EC2 Instance or ECS Task roles for credentials. If
possible you should provide AWS credentials this way instead of adding
them to the shibboleth2.xml file. However, if you need to you can add
a `<Credentials/>` element with these attributes.

| Name          | Type   | Required? | Description |
| ------------- | ------ | --------- | ----------- |
| accessKeyID   | String | Y         | AWS IAM access key ID. |
| secretKey     | String | Y         | AWS IAM secret key for the access key ID. |
| sessionToken  | String | N         | If acquired from assuming a role, the session token to use. |

## Building

This library builds on macOS, CentOS, and Ubuntu using CMake. It
should be possible to build it on Windows however an effort has not
yet been made to make this work.

### Docker

The preferred way to build the plugin is to use Docker containers.
In the `docker/` subdirectory you can find Dockerfiles for various
platforms. To use the image customize these run parameters:

- Volume /source (readonly): this is the source tree. If you do not
  map this to a directory then the image will clone the current source
  from GitHub. You can customize the clone using `UIUC_SHIBPLUGINS_GITURL`
  and `UIUC_SHIBPLUGINS_GITBRANCH`.
- Volume /output: this is where the build artifacts will be installed
  to. You will want to map this to a directory to get the library.
- Volume /build: this is where intermediary build results are stored.
  Normally you don't need to map this to anything, but if you doing
  development then you can save time rebuilding but creating a
  persistent volume for this directory.

### Manual

To build manually you will need to install these build requirements:

- GCC 4.9, Clang 3.3, or later
- CMake 3.1.0 or later
- Boost
- CURL
- Git
- OpenSSL
- Shibboleth 3.0.2 or later
  - Log4Shib
  - XercesC
  - XMLTooling
- UUID
- zlib

Using CMake to build the project:

1. Create a `build` directory and change to it. You should not build
   in the same directory as the source.
2. Run `cmake /path/to/source`. This will create the CMake files.
3. Run `make`. This should build the project and place the build
   artifacts in `./bin` and `./lib`.

## TODO

- Make this work on Windows. PR's welcome :)
- Have the Docker images build rpm and deb files.
