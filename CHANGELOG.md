# NMOS Node API Implementation Changelog

## 0.10.9
- Add api downgrade function from nmoscommon

## 0.10.8
- Add `api_auth` text record to mDNS announcements

## 0.10.7
- Test code refactor

## 0.10.6
- Fix bug with python 3 list iterators

## 0.10.5
- Fix bug with python 3 division and list iterators

## 0.10.4
- Fix bug when using python 3 where .values() returns dict_values object instead of list

## 0.10.3
- Move NMOS packages from recommends to depends

## 0.10.2
- Use newer cysystemd instead of systemd

## 0.10.1
- Fix incompatibility introduced in facade class during move

## 0.10.0
- Add aggregator and facade classes from nmos-common

## 0.9.2
- Add Python3 linting stage to CI, fix linting

## 0.9.1
- Fix missing files in Python 3 Debian package

## 0.9.0
- Use nmoscommon prefer_hostnames/node_hostname to inform all absolute hrefs

## 0.8.3
- Added linting stage to CI and .flake8 file, fixed linting

## 0.8.2
- Add support for Python 3

## 0.8.1
- Call MDNSUpdater stop now required by nmoscommon

## 0.8.0
- Add basic mechanism to discover current Registration API

## 0.7.1
- Fix handling of mDNS exceptions following nmoscommon changes

## 0.7.0
- Add mechanism to disable P2P mode support and mDNS announcement

## 0.6.0
- Disable v1.0 API when running in HTTPS mode

## 0.5.0
- Add provisional support for IS-04 v1.3

## 0.4.3
- Update method used to access config file

## 0.4.2
- Ensure manifest_href matches current protocol

## 0.4.1
- Fix mDNS announcement port

## 0.4.0
- Add mechanism for external services to register and update clocks

## 0.3.2
- Fixes handling of virtual interfaces via Node /self resource

## 0.3.1
- Fixed test failures when run with some discovery methods

## 0.3.0
- Fixed tests to pass under Python 3

## 0.2.0
- Added initial repository state
