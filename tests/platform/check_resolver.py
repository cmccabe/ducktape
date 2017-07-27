# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from ducktape.platform.resolver import Resolver

def append_abracadabra(prefix=""):
    return "%sabracadabra" % prefix

class ResolverExampleBase(object):
    def __init__(self):
        pass

class ResolverExampleDerived(ResolverExampleBase):
    def __init__(self, foo):
        super(ResolverExampleDerived, self).__init__()
        self.foo = foo

class CheckResolver(object):
    def check_resolver_invoke(self):
        resolver = Resolver("tests.platform")
        result = resolver.invoke("check_resolver", "append_abracadabra", prefix="123")
        assert result == "123abracadabra"

    def check_resolver_create(self):
        resolver = Resolver("tests.platform")
        example = resolver.create("ResolverExampleDerived", ResolverExampleBase, foo="foo")
        assert example.foo == "foo"
