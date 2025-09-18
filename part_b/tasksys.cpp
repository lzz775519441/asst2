#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads)
{
}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    killed_ = false;
    mutex_ = new std::mutex();
    waiting_queue_mutex_ = new std::mutex();
    ready_queue_mutex_ = new std::mutex();
    bulk_task_id_ = 0;
    finished_id_ = -1;
    num_threads_ = num_threads;
    threadsPool_ = new std::thread[num_threads_];
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadRunner, this);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    killed_ = true;
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i].join();
    delete[] threadsPool_;
    delete waiting_queue_mutex_;
    delete mutex_;
    delete ready_queue_mutex_;
}

void TaskSystemParallelThreadPoolSleeping::threadRunner()
{
    int task_id;
    int total;
    bulkTask *currentTask;
    while (!killed_)
    {
        ready_queue_mutex_->lock();
        if (ready_queue_.empty())
        {
            waiting_queue_mutex_->lock();
            while (!waiting_queue_.empty())
            {
                bulkTask currentTask = waiting_queue_.top();
                if (currentTask.deps_id > finished_id_)
                    break;
                waiting_queue_.pop();
                ready_queue_.push(std::move(currentTask));
            }
            waiting_queue_mutex_->unlock();
            ready_queue_mutex_->unlock();
        }
        else
        {
            currentTask = &ready_queue_.front();
            ready_queue_mutex_->unlock();

            mutex_->lock();
            if (currentTask->left_task == 0)
            {
                mutex_->unlock();
                continue;
            }
            task_id = currentTask->num_total_tasks - currentTask->left_task;
            total = currentTask->num_total_tasks;
            currentTask->left_task--;
            mutex_->unlock();

            currentTask->runnable->runTask(task_id, total);

            mutex_->lock();
            currentTask->finished_task++;
            if (currentTask->finished_task == currentTask->num_total_tasks)
            {
                ready_queue_mutex_->lock();
                finished_id_ = std::max(finished_id_, currentTask->task_id);
                ready_queue_.pop();
                if (ready_queue_.empty())
                {
                    ready_queue_mutex_->unlock();
                    mutex_->unlock();
                    continue;
                }
                currentTask = &ready_queue_.front();
                ready_queue_mutex_->unlock();
                mutex_->unlock();
            }
            else
                mutex_->unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    bulkTask newTask;
    newTask.runnable = runnable;
    newTask.num_total_tasks = num_total_tasks;
    newTask.deps = deps;
    newTask.task_id = bulk_task_id_;
    newTask.finished_task = 0;
    newTask.left_task = num_total_tasks;
    newTask.deps_id = -1;
    auto it = std::max_element(deps.begin(), deps.end());
    if (it != deps.end())
        newTask.deps_id = *it;

    waiting_queue_mutex_->lock();
    waiting_queue_.push(std::move(newTask));
    waiting_queue_mutex_->unlock();

    return bulk_task_id_++;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    while (true)
    {
        ready_queue_mutex_->lock();
        waiting_queue_mutex_->lock();
        if (ready_queue_.empty() && waiting_queue_.empty())
        {
            waiting_queue_mutex_->unlock();
            ready_queue_mutex_->unlock();
            break;
        }
        waiting_queue_mutex_->unlock();
        ready_queue_mutex_->unlock();
    }
}
