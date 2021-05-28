#  Copyright (c) 2021.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import importlib


def class_for_name(class_name: str):
	"""
	Import service file
	:param class_name: CamelCase class name from healthcheck_python.service
	:return: <class>
	"""
	module_name = importlib.import_module("healthcheck_python")
	submodule_name = getattr(module_name, "service")
	class_ = getattr(submodule_name, class_name)
	return class_
