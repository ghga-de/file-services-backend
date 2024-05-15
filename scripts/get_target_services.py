#!/usr/bin/env python3

# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Dirty script to get target service from branch name"""

import json
import subprocess

TEMP_ALL = ["pcs", "ifrs", "irs"]  # just here for convenience during dev


def get_current_branch_name():
    try:
        # Run the git command to get the current branch name
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        # Return the branch name, stripping any trailing newline
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        exit(1)


def main():
    branch = get_current_branch_name()
    service_tag = branch.split("/")[1]  # handle better
    services = [service_tag] if service_tag in TEMP_ALL else TEMP_ALL
    print(json.dumps(services))


if __name__ == "__main__":
    main()
