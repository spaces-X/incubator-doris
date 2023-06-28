// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/status.h"
#include "tools/build_segment_tool/build_helper.h"
#include "tools/build_segment_tool/JNI/segment_builder_jni.h"
#include "util/doris_metrics.h"
#include "util/uid_util.h"


std::string jstringToString(JNIEnv* env, jstring jstr) {
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return result;
}

JNIEXPORT void JNICALL Java_JniExample_segmentBuild(JNIEnv * env, jobject obj, jstring meta_file_path, jstring data_dir) {
    std::string FLAGS_meta_file = jstringToString(env, meta_file_path);
    std::string FLAGS_data_path = jstringToString(env, data_dir);
    std::string FLAGS_format = "parquet";
    LOG(INFO) << "meta file:" << FLAGS_meta_file << " data path:" << FLAGS_data_path
              << " format:" << FLAGS_format;
    std::string build_dir = FLAGS_data_path;
    //
    auto t0 = std::chrono::steady_clock::now();
    doris::DorisMetrics::instance()->initialize(false, {}, {});
    doris::BuildHelper* instance = doris::BuildHelper::init_instance();
    instance->initial_build_env();
    instance->open(FLAGS_meta_file, build_dir, FLAGS_data_path, FLAGS_format);
    instance->build();
    auto t1 = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> d {t1 - t0};
    LOG(INFO) << "total cost:" << d.count() << " ms";
    // std::exit(EXIT_SUCCESS);
    gflags::ShutDownCommandLineFlags();
}
