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
from ducktape.platform.loader import Loader

def append_abracadabra(prefix=""):
    return "%sabracadabra" % prefix

class LoaderExampleBase(object):
    def __init__(self):
        pass

class LoaderExampleDerived(LoaderExampleBase):
    def __init__(self, foo):
        super(LoaderExampleDerived, self).__init__()
        self.foo = foo

class CheckLoader(object):
    def check_loader_invoke(self):
        loader = Loader("tests.platform")
        result = loader.invoke("check_loader", "append_abracadabra", prefix="123")
        assert result == "123abracadabra"

    def check_loader_create(self):
        loader = Loader("tests.platform")
        example = loader.create("LoaderExampleDerived", "LoaderExampleBase", foo="foo")
        assert example.foo == "foo"
