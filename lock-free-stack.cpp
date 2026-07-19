// From https://www.modernescpp.com/index.php/a-lock-free-stack-a-simple-garbage-collector/

#include <atomic>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <stdlib.h>
#include <utility>
#include <mutex>
#include <vector>

using namespace std;

template<typename T>
class LockFreeStack {
private:
    struct Node {
        T data;
        Node* next;
        Node(T d): data(d), next(nullptr){ }
    };

    std::atomic<Node*> head{nullptr}; // top of stack
    std::atomic<int> topAndPopCounter{0}; // number of threads currently executing topAndPop() 

    // the problem:
    // thread may call pop() and get referenced to the top, be descheduled, and another thread pops the top node and deletes it
    // now the first thread will have use-after-free

    std::atomic<Node*> toBeDeletedNodes{nullptr};  

    void tryToDelete(Node* oldHead) {      
        /* 
        if more than one topAndPop() is in progress, 
        do not delete nodes immediately, 
        instead put them on a retired list. 
        when only one popper remains, it can safely clean up.
        */
        if (topAndPopCounter == 1) {  
            Node* copyOfToBeDeletedNodes = toBeDeletedNodes.exchange(nullptr);
            if (topAndPopCounter == 1) 
                deleteAllNodes(copyOfToBeDeletedNodes); 
            else 
                addNodeToBeDeletedNodes(copyOfToBeDeletedNodes); 
            delete oldHead;
        } else {
            addNodeToBeDeletedNodes(oldHead);  
        }
    }

    void addNodeToBeDeletedNodes(Node* oldHead) { 
        oldHead->next = toBeDeletedNodes;
        while( !toBeDeletedNodes.compare_exchange_strong(oldHead->next, oldHead) ); 
    }

    void deleteAllNodes(Node* currentNode) {  
        while (currentNode) {
            Node* nextNode = currentNode->next;
            delete currentNode;
            currentNode = nextNode;
        }
    }
public:
    LockFreeStack() = default;
    LockFreeStack(const LockFreeStack&) = delete;
    LockFreeStack& operator= (const LockFreeStack&) = delete;
    ~LockFreeStack() {
        deleteAllNodes(head.load());
        deleteAllNodes(toBeDeletedNodes.load());
    }

    void push(T val) { // Treiber stack push
        Node* const newNode = new Node(val); 
        newNode->next = head.load();
        while( !head.compare_exchange_strong(newNode->next, newNode) );
    }

    T pop() { // Remove top node from stack, reclaim it later
        ++topAndPopCounter;
        Node* oldHead = head.load();
        while( oldHead && !head.compare_exchange_strong(oldHead, oldHead->next) ) {
            if ( !oldHead ) throw std::out_of_range("The stack is empty!");
        }
        if (!oldHead) throw std::out_of_range("The stack is empty");
        auto topElement = oldHead->data;
        tryToDelete(oldHead); 
        --topAndPopCounter; 
        return topElement;
    }
    bool tryPop(T& v) {
        ++topAndPopCounter;
        Node* oldHead = head.load();
        while (oldHead && !head.compare_exchange_strong(oldHead, oldHead->next)) ;
        if (oldHead == nullptr) {
            --topAndPopCounter;
            return false;
        }
        v = oldHead->data;
        tryToDelete(oldHead);
        --topAndPopCounter;
        return true;
    }
};
   
// Test case harness
int main(int argc, char* argv[]){
    if (argc != 4) {
        std::cout << "Usage: ./lock-free-stack <num_producers> <num_consumers> <num_msg>\n";
        return 1;
    }
    const long NUM_PROD = strtol(argv[1], NULL, 10), NUM_CONS = strtol(argv[2], NULL, 10), NUM_MSG = strtol(argv[3], NULL, 10);

    std::mutex cout_mtx;

    LockFreeStack<std::pair<int, int>> lockFreeStack;
    std::atomic<long> push_cnt = 0, pop_cnt = 0;
    std::vector<std::future<void>> futures;

    for (int i = 0; i < NUM_CONS; i++) {
        futures.push_back(std::async(std::launch::async, [&, i]() {
            while (pop_cnt.load(std::memory_order_relaxed) != NUM_PROD * NUM_MSG) {
                std::pair<int, int> p {};
                if (lockFreeStack.tryPop(p)) {
                    std::unique_lock lk(cout_mtx);
                    std::cout << "reader " << i << " received (producer=" <<  p.first << ", msg=" << p.second << ")\n";
                    pop_cnt.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }));
    }

    for (int i = 0; i < NUM_PROD; i++) {
        futures.push_back(std::async(std::launch::async, [&, i]() {
            for (int j = 0; j < NUM_MSG; j++) {
                lockFreeStack.push({i, j});
                push_cnt.fetch_add(1, std::memory_order_relaxed);
            }
        }));
    }

    for (auto& f : futures) f.get();

    std::cout << "pushed: " << push_cnt.load() << ", popped: " << pop_cnt.load() << '\n'; 
}
