#include <atomic>
#include <condition_variable>
#include <functional>
#include <type_traits>
#include <queue>
#include <mutex>
#include <future>
#include <algorithm>
#include <iostream>

// FIFO threadpool
class threadpool {
private:
    std::vector<std::thread> threads;
    std::queue<std::function<void()>> q;
    std::mutex mtx; 
    std::condition_variable cv;
    bool stopping = false;
    int n;
public:
    threadpool(int num_threads) {
        if (num_threads < 1) throw std::invalid_argument("number of threads must be greater than 0");
        int max_threads = std::thread::hardware_concurrency();
        n = std::min(num_threads, max_threads);
        for (int i = 0; i < n; i++) {
            threads.emplace_back([this]() {
                while (true) {
                    std::unique_lock<std::mutex> lk(mtx);
                    cv.wait(lk, [this]{ return stopping || !q.empty(); });
                    if (stopping && q.empty()) break;
                    std::function<void()> task = std::move(q.front());
                    q.pop();
                    task();
                }
            });
        }
    }
    threadpool(const threadpool& other) = delete;
    threadpool& operator=(const threadpool& other) = delete;
    ~threadpool() {
        stopping = true;
        cv.notify_all();
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
    }
    template<typename F, typename... Args>
    std::future<typename std::invoke_result_t<F, Args...>> post(F&& f, Args&&... args) {
        using ReturnType = std::invoke_result_t<F, Args...>;
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            [f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable {
                return f(std::move(args)...);
            }
        );
        std::future<ReturnType> fut = task->get_future();
        std::lock_guard<std::mutex> lk(mtx);
        q.emplace([task]() mutable {
            (*task)();
        });
        return fut;
    }
};

int main() {
    threadpool executor{4};
    auto task1 = [](int a, int b) { 
        std::cout << "Task 1 running, a = " << a << ", b = " << b << ", a + b = " << a + b << std::endl;
    };
    auto task2 = [](int a) {
        std::cout << "Task 2 running, a = " << a << ", a * 2 = " << a * 2 << std::endl;
    };
    auto task3 = []() -> int {
        std::cout << "Task 3 running" << std:: endl;
        return 42;
    };
    (void) executor.post(task1, 5, 10);
    (void) executor.post(task2, 100);
    auto fut = executor.post(task3);
    std::cout << "Return from Task 3: " << fut.get() << std::endl;
}
