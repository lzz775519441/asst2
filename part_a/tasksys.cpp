#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
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
    num_threads_ = num_threads;
    threadsPool_ = new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn()
{
    delete[] threadsPool_;
}

void TaskSystemParallelSpawn::threadRunner(int num_Total_tasks, int *current_task, std::mutex *mutex, IRunnable *runnable)
{
    while (true)
    {
        mutex->lock();
        if (*current_task >= num_Total_tasks)
        {
            mutex->unlock();
            break;
        }
        int task_id = *current_task;
        (*current_task)++;
        mutex->unlock();
        runnable->runTask(task_id, num_Total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    std::mutex mutex;
    int current_task = 0;
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i] = std::thread(&TaskSystemParallelSpawn::threadRunner, this, num_total_tasks, &current_task, &mutex, runnable);
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i].join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
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
    finished_tasks_ = 0;
    mutex_ = new std::mutex();
    done_ = new std::condition_variable();
    num_total_tasks_ = 0;
    finished_ = false;
    num_threads_ = num_threads;
    remainingTasks_ = 0;
    threadsPool_ = new std::thread[num_threads];
    runnable_ = nullptr;
    done_mutex_ = new std::mutex();
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::threadRunner, this);
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    finished_ = true;
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i].join();
    delete[] threadsPool_;
    delete mutex_;
    delete done_;
    delete done_mutex_;
}

void TaskSystemParallelThreadPoolSpinning::threadRunner()
{
    int task_id;
    while (!finished_)
    {
        mutex_->lock();
        if (remainingTasks_ > 0)
        {
            task_id = num_total_tasks_ - remainingTasks_;
            remainingTasks_--;
            mutex_->unlock();
            runnable_->runTask(task_id, num_total_tasks_);
            mutex_->lock();
            finished_tasks_++;
            if (finished_tasks_ == num_total_tasks_)
            {
                mutex_->unlock();
                done_mutex_->lock();
                done_mutex_->unlock();
                done_->notify_all();
            }
            else
                mutex_->unlock();
        }
        else
            mutex_->unlock();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    std::unique_lock<std::mutex> lk(*done_mutex_);
    mutex_->lock();
    finished_tasks_ = 0;
    runnable_ = runnable;
    remainingTasks_ = num_total_tasks;
    num_total_tasks_ = num_total_tasks;
    mutex_->unlock();
    done_->wait(lk);
    lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
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
    mutex_ = new std::mutex();
    done_mutex_ = new std::mutex();
    job_coming_mutex_ = new std::mutex();
    job_coming_ = new std::condition_variable();
    done_ = new std::condition_variable();
    threadsPool_ = new std::thread[num_threads];
    num_threads_ = num_threads;
    num_total_tasks_ = 0;
    remainingTasks_ = 0;
    finished_tasks_ = 0;
    finished_ = false;
    runnable_ = nullptr;
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadRunner, this);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    finished_ = true;
    job_coming_mutex_->lock();
    job_coming_mutex_->unlock();
    job_coming_->notify_all();
    for (int i = 0; i < num_threads_; i++)
        threadsPool_[i].join();
    delete[] threadsPool_;
    delete mutex_;
    delete done_mutex_;
    delete job_coming_mutex_;
    delete done_;
    delete job_coming_;
}

void TaskSystemParallelThreadPoolSleeping::threadRunner()
{
    int task_id;
    while (!finished_)
    {
        mutex_->lock();
        if (remainingTasks_ > 0)
        {
            task_id = num_total_tasks_ - remainingTasks_;
            remainingTasks_--;
            mutex_->unlock();

            runnable_->runTask(task_id, num_total_tasks_);

            mutex_->lock();
            finished_tasks_++;
            if (finished_tasks_ == num_total_tasks_)
            {
                mutex_->unlock();

                done_mutex_->lock();
                done_mutex_->unlock();
                done_->notify_all();
            }
            else
                mutex_->unlock();
        }
        else
        {
            mutex_->unlock();

            std::unique_lock<std::mutex> lk(*job_coming_mutex_);
            job_coming_->wait(lk);
            lk.unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    std::unique_lock<std::mutex> lk(*done_mutex_);

    mutex_->lock();
    finished_tasks_ = 0;
    runnable_ = runnable;
    remainingTasks_ = num_total_tasks;
    num_total_tasks_ = num_total_tasks;
    mutex_->unlock();

    job_coming_mutex_->lock();
    job_coming_mutex_->unlock();
    job_coming_->notify_all();

    done_->wait(lk);
    lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
