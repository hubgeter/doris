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

#include "runtime/workload_management/workload_action.h"

#include "runtime/fragment_mgr.h"

namespace doris {

void WorkloadActionCancelQuery::exec(WorkloadAction::RuntimeContext* action_runtime_ctx) {
    std::stringstream msg;
    msg << "Query " << print_id(action_runtime_ctx->resource_ctx->task_controller()->task_id())
        << " cancelled by workload policy: " << action_runtime_ctx->policy_name
        << ", id:" << action_runtime_ctx->policy_id << ", " << action_runtime_ctx->cond_eval_msg;
    std::string msg_str = msg.str();
    LOG(INFO) << "[workload_schedule]" << msg_str;
    ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
            action_runtime_ctx->resource_ctx->task_controller()->task_id(),
            Status::InternalError<false>(msg_str));
}

void WorkloadActionMoveQuery::exec(WorkloadAction::RuntimeContext* action_runtime_ctx) {
    LOG(INFO) << "[workload_schedule]move query action run group=" << _wg_name;
};

} // namespace doris