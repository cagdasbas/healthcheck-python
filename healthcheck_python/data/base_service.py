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

class BaseService:
	"""
	Base service template
	All services has to implement this
	"""

	def __init__(self, name):
		self.name = name

	def json(self):
		"""
		Returns all attributes as dict
		:return: dict, all object attributes
		"""
		raise NotImplementedError()

	def add_new_point(self, point):
		"""
		Add new function call
		:param point: dict, new function call data
		"""
		raise NotImplementedError()

	def is_healthy(self, current_time=None):
		"""
		Check if last call is within timeout limits
		:param current_time: time.time() object, Optional, check the status with specific time
		:return: boolean, service status
		"""
		raise NotImplementedError()
