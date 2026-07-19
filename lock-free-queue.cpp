#include <type_traits>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

// Michael-Scott Lock Free MPMC Queue
template<typename T>
requires std::is_default_constructible_v<T>
class LockFreeQueue {
private:
    struct Node {
        T value;
        std::atomic<Node*> next;

        Node() : value(), next(nullptr) {}
        explicit Node(const T& v) : value(v), next(nullptr) {}
        explicit Node(T&& v) : value(std::move(v)), next(nullptr) {}
    };

    struct Queue {
        std::atomic<Node*> head;
        std::atomic<Node*> tail;

        Queue() {
            Node* dummy = new Node();
            head.store(dummy);
            tail.store(dummy);
        }
    };

    std::unique_ptr<Queue> q;
public:
    LockFreeQueue(): q(std::make_unique<Queue>()) {}
    ~LockFreeQueue() {}
    LockFreeQueue(const LockFreeQueue& o) = delete;
    LockFreeQueue(LockFreeQueue&& o) = delete;
    LockFreeQueue& operator=(const LockFreeQueue& o) = delete;
    LockFreeQueue& operator=(LockFreeQueue&& o) = delete;
    
    void enqueue(const T& value) {
        Node* node = new Node(value);

        while (true) {
            Node* tail = q->tail.load();
            Node* next = tail->next.load();

            if (tail == q->tail.load()) {
                if (next == nullptr) {
                    if (tail->next.compare_exchange_weak(next, node)) {
                        q->tail.compare_exchange_strong(tail, node);
                        return;
                    }
                } else {
                    q->tail.compare_exchange_strong(tail, next);
                }
            }
        }
    }

    void enqueue(T&& value) {
        Node* node = new Node(std::move(value));

        while (true) {
            Node* tail = q->tail.load();
            Node* next = tail->next.load();

            if (tail == q->tail.load()) {
                if (next == nullptr) {
                    if (tail->next.compare_exchange_weak(next, node)) {
                        q->tail.compare_exchange_strong(tail, node);
                        return;
                    }
                } else {
                    q->tail.compare_exchange_strong(tail, next);
                }
            }
        }
    }

    bool try_dequeue(T& out) {
        while (true) {
            Node* head = q->head.load();
            Node* tail = q->tail.load();
            Node* next = head->next.load();

            if (head == q->head.load()) {
                if (head == tail) {
                    if (next == nullptr) {
                        return false; // empty
                    }
                    q->tail.compare_exchange_strong(tail, next);
                } else {
                    out = std::move(next->value);
                    if (q->head.compare_exchange_strong(head, next)) {
                        // Do not delete head here without safe reclamation
                        return true;
                    }
                }
            }
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr
            << "Usage: "
            << argv[0]
            << " <producers> <consumers> <messages_per_producer>\n";
        return 1;
    }

    const int producers = std::stoi(argv[1]);
    const int consumers = std::stoi(argv[2]);
    const int messages = std::stoi(argv[3]);

    const int total_messages = producers * messages;

    LockFreeQueue<int> q;

    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    std::atomic<int> finished_producers{0};

    // Count how many times each value appears.
    std::vector<std::atomic<int>> seen(total_messages);
    for (auto& x : seen)
        x.store(0);

    std::vector<std::thread> producer_threads;
    std::vector<std::thread> consumer_threads;

    auto start = std::chrono::steady_clock::now();

    //--------------------------------------
    // Producers
    //--------------------------------------

    for (int p = 0; p < producers; ++p) {
        producer_threads.emplace_back([&, p] {
            int base = p * messages;

            for (int i = 0; i < messages; ++i) {
                q.enqueue(base + i);
                ++produced;
            }

            ++finished_producers;
        });
    }

    //--------------------------------------
    // Consumers
    //--------------------------------------

    for (int c = 0; c < consumers; ++c) {
        consumer_threads.emplace_back([&] {
            while (true) {
                int value;

                if (q.try_dequeue(value)) {
                    ++consumed;
                    ++seen[value];
                }
                else {
                    if (finished_producers.load() == producers &&
                        consumed.load() == total_messages)
                        break;

                    std::this_thread::yield();
                }
            }
        });
    }

    //--------------------------------------
    // Join
    //--------------------------------------

    for (auto& t : producer_threads)
        t.join();

    for (auto& t : consumer_threads)
        t.join();

    auto end = std::chrono::steady_clock::now();

    //--------------------------------------
    // Verify
    //--------------------------------------

    bool ok = true;

    if (produced != total_messages) {
        std::cout << "Produced mismatch!\n";
        ok = false;
    }

    if (consumed != total_messages) {
        std::cout << "Consumed mismatch!\n";
        ok = false;
    }

    int duplicates = 0;
    int missing = 0;

    for (int i = 0; i < total_messages; ++i) {
        int cnt = seen[i].load();

        if (cnt == 0)
            ++missing;
        else if (cnt > 1)
            duplicates += cnt - 1;
    }

    if (missing)
        std::cout << "Missing messages: " << missing << '\n';

    if (duplicates)
        std::cout << "Duplicate messages: " << duplicates << '\n';

    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "\n========== RESULTS ==========\n";
    std::cout << "Produced : " << produced << '\n';
    std::cout << "Consumed : " << consumed << '\n';
    std::cout << "Missing  : " << missing << '\n';
    std::cout << "Duplicate: " << duplicates << '\n';
    std::cout << "Time     : " << elapsed.count() << " ms\n";

    if (ok && missing == 0 && duplicates == 0)
        std::cout << "PASS\n";
    else
        std::cout << "FAIL\n";
}
