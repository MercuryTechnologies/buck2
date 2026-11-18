# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@shim//build_defs:python_binary.bzl", _python_binary = "python_binary")
load("@shim//build_defs:python_library.bzl", _python_library = "python_library")

# The package defaults `PACKAGE` files set with these configure Meta's Python
# macros (type checking, opt by default, lazy imports), which open source does
# not have.

def _noop(*_args, **_kwargs):
    pass

python = struct(
    set_opt_by_default = _noop,
    set_py_version_for_type_checking = _noop,
    set_safer_lazy_imports = _noop,
    set_typing = _noop,
)

python_binary = _python_binary
python_library = _python_library
