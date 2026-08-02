// Based on https://github.com/173duprot/mcmpq.h/blob/main/mcmpq.h

#include <atomic> 
#include <vector>
#include <memory>
#include <thread>
#include <iostream>
#include <chrono>

using namespace std;

template<typename T>
struct mpmc_queue {
public:
    size_t capacity_;
    mpmc_queue(size_t capacity): 
        capacity_(capacity), slots_(make_unique<slot[]>(capacity)) 
    {}
    
    template<typename U> 
    void enqueue(U&& t) {
        size_t head = head_.fetch_add(1, std::memory_order_acq_rel);
        slot* s = static_cast<slot*>(&slots_[(head % capacity_)]);
        while ((head / capacity_) * 2 != s->turn.load(std::memory_order_acquire)); // busy wait
        s->data = make_unique<T>(std::forward<U>(t));
        s->turn.store((head / capacity_) * 2 + 1, std::memory_order_release);
    }
    
    unique_ptr<T> dequeue() {
        size_t tail = tail_.fetch_add(1, std::memory_order_acq_rel);
        slot* s = &slots_[tail % capacity_];
        while ((tail / capacity_) * 2 + 1 != s->turn.load(std::memory_order_acquire)); // busy wait
        unique_ptr<T> ret = std::move(s->data);
        s->turn.store((tail / capacity_) * 2 + 2,   std::memory_order_release);
        return std::move(ret);
    }
    
    template<typename U>
    bool try_enqueue(U&& t) {
        size_t head = head_.load(std::memory_order_acquire);
        for (;;) {
            slot* s = &slots_[head % capacity_];
            if (s->turn.load(std::memory_order_acquire) == (head / capacity_) * 2) {
                if (head_.compare_exchange_strong(head, head + 1, std::memory_order_release)) { // succesfully update
                    s->data = make_unique<T>(std::forward<U>(t));
                    s->turn.store((head / capacity_) * 2 + 1, 
                        std::memory_order_release);
                    return true;
                }
            } else {
                size_t old_head = head;
                head = head_.load(std::memory_order_acquire);
                if (head == old_head) {
                    return false;
                }
            }   
        }
    }
    
    unique_ptr<T> try_dequeue() {
        size_t tail = tail_.load(std::memory_order_acquire);
        for (;;) {
            slot* s = &slots_[tail % capacity_];
            if (s->turn.load(std::memory_order_acquire) == (tail / capacity_) * 2 + 1) {
                if (tail_.compare_exchange_strong(tail, tail + 1, std::memory_order_release)) {
                    unique_ptr<T> ret = std::move(s->data);
                    s->turn.store((tail / capacity_) * 2 + 2, std::memory_order_release);
                    return std::move(ret);
                }
            } else {
                size_t old_tail = tail;
                tail = tail_.load(std::memory_order_acquire);
                if (tail == old_tail) {
                    return nullptr;
                }
            }   
        }
    }
    
    mpmc_queue(const mpmc_queue&) = delete;
    mpmc_queue(mpmc_queue&&) = delete;
    void operator=(const mpmc_queue&) = delete;
    void operator=(mpmc_queue&&) = delete;
private:
    struct slot {
        atomic<size_t> turn {0};
        unique_ptr<T> data {nullptr};
    };
    unique_ptr<slot[]> slots_; // can't use vector as slot struct is non-copyable due to atomic turn
    atomic<size_t> head_{0}, tail_{0};
};

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cout << "usage: ./mpmc-queue <num_threads> <num_message> <capacity>" << endl;
        return 1;
    }
    const size_t NUM_THREADS = strtoul(argv[1], nullptr, 10);
    const size_t NUM_MESSAGES = strtoul(argv[2], nullptr, 10);
    const size_t CAPACITY = strtoul(argv[3], nullptr, 10);
    mpmc_queue<int> q {CAPACITY};

    vector<thread> producers, consumers;
    atomic<int> enqueue_cnt{0}, dequeue_cnt {0};

    auto start = chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_THREADS; i++) {
        producers.push_back(thread([&] {
            for (size_t j = 0; j < NUM_MESSAGES; j++) {
                q.enqueue(j);
                enqueue_cnt++;
            }
        }));
    }
    for (size_t i = 0; i < NUM_THREADS; i++) {
        consumers.push_back(thread([&]{
            for (size_t j = 0; j < NUM_MESSAGES; j++) {
                auto p = q.dequeue();
                dequeue_cnt++;
            }
        }));
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start).count();

    cout << "threads: " << NUM_THREADS << "\n"
         << "enqueued: " << enqueue_cnt << "\n"
         << "dequeued: " << dequeue_cnt << endl;
    cout << "Took: " << duration << "ms" << endl;
}
