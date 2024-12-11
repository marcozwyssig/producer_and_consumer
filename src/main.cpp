#include <iostream>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include <atomic>
#include <future>
#include <sstream>
#include <optional>

// Utility for logging with thread ID
void log(const std::string& message) {
    std::ostringstream oss;
    oss << "[Thread " << std::this_thread::get_id() << "] " << message << std::endl;
    std::cout << oss.str();
}

class MutexLocker {
public:
    // Constructor creates an internal mutex
    MutexLocker() = default;

    // Method to invoke a task within a lock
    template <typename Callable>
    auto runWithLockGuard(Callable&& task) {
        std::lock_guard<std::mutex> lock(mtx);
        return task();
    }

    template <typename Callable>
    auto runWithUniqueLock(Callable&& task) {
        std::unique_lock<std::mutex> lock(mtx);
        return task(std::move(lock)); // Pass the unique_lock to the task
    }

private:
    std::mutex mtx; // Internal mutex
};


// Event structure
struct Event {
    int producerId;
    std::string topic;
    int eventId;
    std::string changeData;
};

// Observer interface
class Observer {
public:
    virtual void update(const Event& event) = 0;
    virtual ~Observer() = default;
};

// Observable (Subject) class
class Observable {
private:
    std::unordered_map<std::string, std::vector<std::shared_ptr<Observer>>> topicObservers;
    MutexLocker locker;

public:
    Observable& addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        locker.runWithLockGuard([&] {
            topicObservers[topic].push_back(observer);
            log("Added observer to topic: " + topic);
            return this;
        });
        return *this;
    }

    void notifyObservers(const Event& event) {
        locker.runWithLockGuard([&] {
            log("Notifying observers for topic: " + event.topic + ", Event ID: " + std::to_string(event.eventId));
            if (topicObservers.find(event.topic) != topicObservers.end()) {
                for (auto& observer : topicObservers[event.topic]) {
                    if (observer) {
                        observer->update(event);
                    }
                }
            }
        });
    }
};

// Event ID Generator for managing topic-specific event counters
class EventIdGenerator {
private:
    std::unordered_map<std::string, int> topicEventCounters;
    MutexLocker locker;

public:
    int getNextEventId(const std::string& topic) {
        return locker.runWithLockGuard([&] {
            int& counter = topicEventCounters[topic]; // Access or initialize counter for topic
            return ++counter; // Increment and return the counter
        });
    }
};

// Concrete Observer (Consumer)
class Consumer : public Observer {

private:
    int id; // Unique consumer ID
    std::queue<Event> eventQueue;
    MutexLocker locker;
    std::condition_variable cv;
    std::atomic<bool> running{true};

public:
    explicit Consumer(int id) : id(id) {
        log("Consumer " + std::to_string(id) + " created.");
    }

    void update(const Event& event) override {
        locker.runWithLockGuard([&] {
            eventQueue.push(event);
            log("Consumer " + std::to_string(id) + " received event: Producer " +
                std::to_string(event.producerId) + ", Topic " + event.topic +
                ", Event " + std::to_string(event.eventId));
        });
        cv.notify_one();
    }

    void processEvents() {
        while (running) {
            std::optional<Event> eventOpt = locker.runWithUniqueLock([&](std::unique_lock<std::mutex> lock) {
                // Wait for a new event or stop signal
                cv.wait(lock, [this] {
                    return !eventQueue.empty() || !running;
                });

                // Exit if no events are left and stop signal is received
                if (!running && eventQueue.empty()) {
                    return std::optional<Event>{}; // Return an empty optional
                }

                // Retrieve and remove the event from the queue
                Event event = eventQueue.front();
                eventQueue.pop();
                return std::optional<Event>{event};
            });

            // Break the loop if no event was retrieved
            if (!eventOpt.has_value()) {
                break;
            }

            // Process the event outside the critical section
            const auto& event = eventOpt.value();
            log("Consumer " + std::to_string(id) + " processed event: Producer " +
                std::to_string(event.producerId) + ", Topic " + event.topic +
                ", Event " + std::to_string(event.eventId) +
                ", Change: " + event.changeData);
        }

        log("Consumer " + std::to_string(id) + " stopped processing events.");
    }


    void stop() {
        running = false;
        cv.notify_all();
        log("Consumer " + std::to_string(id) + " stop signal issued.");
    }
};

// Producer class (Uses Observable)
class Producer : public Observable {
private:
    int id;
    std::future<void> producerFuture;
    EventIdGenerator& eventGenerator; // Shared event ID generator

public:
    explicit Producer(int id, EventIdGenerator& generator) 
        : id(id), eventGenerator(generator) 
    {}

    Producer& addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        Observable::addObserver(topic, observer);
        return *this;
    }

    Producer& produceAsync(const std::string& topic, int numEvents) {
        producerFuture = std::async(std::launch::async, [this, topic, numEvents]() {
            for (int i = 0; i < numEvents; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5000)); // Simulate work
                
                // Get the next unique event ID for the topic
                int eventId = eventGenerator.getNextEventId(topic);

                Event event{id, topic, eventId, "Generated event " + std::to_string(eventId)};
                log("Producer " + std::to_string(id) + " generated event: " +
                    std::to_string(eventId) + " on topic " + topic);
                notifyObservers(event);
            }
        });
        return *this;
    }

    void wait() {
        if (producerFuture.valid()) {
            producerFuture.get();
        }
        log("Producer " + std::to_string(id) + " finished producing.");
    }

    Producer& startProducer(const std::string& topic, int numEvents) {
        return produceAsync(topic, numEvents);
    }
};

// Orchestrator class for producers and consumers
class ProducerConsumerOrchestrator {
private:
    int consumerCounter = 0; // Counter to assign unique IDs to consumers
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::vector<std::thread> consumerThreads;

public:
    std::shared_ptr<Consumer> createConsumer() {
        auto consumer = std::make_shared<Consumer>(++consumerCounter);
        consumers.push_back(consumer);
        return consumer;
    }

    Producer& createProducer(int id, EventIdGenerator& generator) {
        auto producer = std::make_shared<Producer>(id, generator);
        producers.push_back(producer);
        log("Created a new producer with ID: " + std::to_string(id));
        return *producer;
    }

    ProducerConsumerOrchestrator& startConsumers() {
        log("Starting consumers...");
        for (auto& consumer : consumers) {
            consumerThreads.emplace_back([consumer]() { consumer->processEvents(); });
        }
        return *this;
    }

    ProducerConsumerOrchestrator& waitForProducers() {
        log("Waiting for producers to finish...");
        for (auto& producer : producers) {
            producer->wait();
        }
        return *this;
    }

    ProducerConsumerOrchestrator& stopConsumers() {
        log("Stopping consumers...");
        for (auto& consumer : consumers) {
            consumer->stop();
        }
        
        for (auto& thread : consumerThreads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        return *this;
    }
};

// Main function
int main() {
    log("Starting producer-consumer orchestrator.");
    EventIdGenerator eventGenerator; // Shared event generator
    ProducerConsumerOrchestrator orchestrator;

    auto consumer1 = orchestrator.createConsumer(); // Consumer 1
    auto consumer2 = orchestrator.createConsumer(); // Consumer 2

    orchestrator
        .createProducer(1, eventGenerator)
        .addObserver("topicA", consumer1)
        .addObserver("topicB", consumer2)
        .startProducer("topicA", 2);

    orchestrator
        .createProducer(2, eventGenerator)
        .addObserver("topicA", consumer1)
        .addObserver("topicB", consumer2)
        .startProducer("topicB", 3);

    orchestrator
        .startConsumers()
        .waitForProducers()
        .stopConsumers();

    log("Producer-consumer orchestrator finished.");
    return 0;
}