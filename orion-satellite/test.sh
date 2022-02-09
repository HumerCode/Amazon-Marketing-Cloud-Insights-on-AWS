#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x

PIPELINE = aws codepipeline list-pipeline-executions --pipeline-name orion-satellite-pipeline --max-items 1
echo $PIPELINE