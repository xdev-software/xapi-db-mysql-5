[![Latest version](https://img.shields.io/maven-central/v/com.xdev-software/xapi-db-mysql-5)](https://mvnrepository.com/artifact/com.xdev-software/xapi-db-mysql-5)
[![Build](https://img.shields.io/github/workflow/status/xdev-software/xapi-db-mysql-5/Check%20Build/develop)](https://github.com/xdev-software/xapi-db-mysql-5/actions/workflows/checkBuild.yml?query=branch%3Adevelop)
[![javadoc](https://javadoc.io/badge2/com.xdev-software/xapi-db-mysql-5/javadoc.svg)](https://javadoc.io/doc/com.xdev-software/xapi-db-mysql-5) 

# SqlEngine Database Adapter MySQL 5

The XDEV Application Framework provides an abstraction over database dialects as part of its SqlEngine. This module is the Database Adapter for MySQL 5 witch includes the MySQL-specific implementation for database access.


## XDEV-IDE
XDEV(-IDE) is a visual Java development environment for fast and easy application development (RAD - Rapid Application Development). XDEV differs from other Java IDEs such as Eclipse or NetBeans, focusing on programming through a far-reaching RAD concept. The IDE's main components are a Swing GUI builder, the XDEV Application Framework, and numerous drag-and-drop tools and wizards with which the functions of the framework can be integrated.

The XDEV-IDE was license-free up to version 4 inclusive and is available for Windows, Linux and macOS. From version 5, the previously proprietary licensed additional modules are included in the IDE and the license of the entire product has been converted to a paid subscription model. The XDEV Application Framework, which represents the core of the RAD concept of XDEV and is part of every XDEV application, was released as open-source in 2008.

## Contributing

We would absolutely love to get the community involved, and we welcome any form of contributions – comments and questions on different communication channels, issues and pull request in the repositories, and anything that you build and share using our components.

### Get in touch with the team

Twitter: https://twitter.com/xdevsoftware<br/>
Mail: opensource@xdev-software.de

### Some ways to help:

- **Report bugs**: File issues on GitHub.
- **Send pull requests**: If you want to contribute code, check out the development instructions below.

We encourage you to read the [contribution instructions by GitHub](https://guides.github.com/activities/contributing-to-open-source/#contributing) also.

## Dependencies and Licenses
The XDEV Application Framework is released under [GNU Lesser General Public License version 3](https://www.gnu.org/licenses/lgpl-3.0.en.html) aka LGPL 3<br/>
View the [summary of all dependencies online](https://xdev-software.github.io/xapi-db-mysql-5/dependencies/)

## Releasing [![Build](https://img.shields.io/github/workflow/status/xdev-software/xapi-db-mysql-5/Release?label=Release)](https://github.com/xdev-software/xapi-db-mysql-5/actions/workflows/release.yml)

Before releasing:
* Consider doing a [test-deployment](https://github.com/xdev-software/xapi-db-mysql-5/actions/workflows/test-deploy.yml?query=branch%3Adevelop) before actually releasing.
* Check the [changelog](CHANGELOG.md)

If the ``develop`` is ready for release, create a pull request to the ``master``-Branch and merge the changes.

When the release is finished do the following:
* Merge the auto-generated PR (with the incremented version number) back into the ``develop``
* Link the corresponding version from the [changelog](CHANGELOG.md) to the [GitHub release](https://github.com/xdev-software/xapi-db-mysql-5/releases/latest)
