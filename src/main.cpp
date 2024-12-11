/**
 * A multi-threaded Producer-Consumer example using C++.
 * 
 * Features:
 * - Observers for event handling.
 * - Mutex locking for thread safety.
 * - Condition variables for consumer synchronization.
 * - Async producer event generation.
 * 
 * Classes:
 * - MutexLocker: Encapsulates mutex functionality for thread safety.
 * - Event: Represents a producer-generated event.
 * - Observer: Interface for consumer update mechanism.
 * - Observable: Manages topic-observer relationships.
 * - EventIdGenerator: Generates unique event IDs for topics.
 * - Consumer: Processes events and implements Observer.
 * - Producer: Generates events and notifies observers.
 * - ProducerConsumerOrchestrator: Manages producers and consumers.
 */

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
#include <functional>

/**
 * Utility function for logging messages.
 */
void log(const std::string& message) {
    std::ostringstream oss;
    oss << "[Thread " << std::this_thread::get_id() << "] " << message << std::endl;
    std::cout << oss.str();
}

/**
 * MutexLocker encapsulates mutex functionality.
 */
class MutexLocker {
private:
    std::mutex mtx;

public:
    template <typename Callable>
    auto runWithLockGuard(Callable&& task) {
        try {
            std::lock_guard<std::mutex> lock(mtx);
            return task();
        } catch (const std::exception& ex) {
            log("Error in runWithLockGuard: " + std::string(ex.what()));
            throw;
        }
    }

    template <typename Callable>
    auto runWithUniqueLock(Callable&& task) {
        try {
            std::unique_lock<std::mutex> lock(mtx);
            return task(std::move(lock));
        } catch (const std::exception& ex) {
            log("Error in runWithUniqueLock: " + std::string(ex.what()));
            throw;
        }
    }
};

/**
 * Event represents a producer-generated event.
 */
struct Event {
    int producerId;
    std::string topic;
    int eventId;
    std::string changeData;
};

/**
 * Observer interface for event updates.
 */
class Observer {
public:
    virtual void update(const Event& event) = 0;
    virtual ~Observer() = default;
};

/**
 * Observable manages observers and event notifications.
 */
class Observable {
private:
    std::unordered_map<std::string, std::vector<std::shared_ptr<Observer>>> topicObservers;
    MutexLocker locker;

public:
    void addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        locker.runWithLockGuard([&]() {
            topicObservers[topic].push_back(observer);
            log("Observer added for topic: " + topic);
        });
    }

    void notifyObservers(const Event& event) {
        locker.runWithLockGuard([&]() {
            if (topicObservers.count(event.topic) > 0) {
                for (auto& observer : topicObservers[event.topic]) {
                    try {
                        if (observer) {
                            observer->update(event);
                        }
                    } catch (const std::exception& ex) {
                        log("Error notifying observer: " + std::string(ex.what()));
                    }
                }
            }
        });
    }
};

/**
 * EventIdGenerator generates unique event IDs per topic.
 */
class EventIdGenerator {
private:
    std::unordered_map<std::string, int> topicEventCounters;
    MutexLocker locker;

public:
    int getNextEventId(const std::string& topic) {
        return locker.runWithLockGuard([&]() {
            return ++topicEventCounters[topic];
        });
    }
};

/**
 * Consumer processes events and implements Observer.
 */
class Consumer : public Observer {
private:
    int id;
    MutexLocker locker;
    std::function<void(const Event&)> consumeHandler;

public:
    Consumer(int id, std::function<void(const Event&)> handler) 
        : id(id), consumeHandler(std::move(handler)) {
        log("Consumer " + std::to_string(id) + " created.");
    }

    void update(const Event& event) override {
        locker.runWithLockGuard([&]() {
            try {
                consumeHandler(event);
            } catch (const std::exception& ex) {
                log("Error in Consumer update: " + std::string(ex.what()));
            }
        });
    }
};

/**
 * Producer generates events and notifies observers.
 */
class Producer : public Observable {
private:
    int id;
    EventIdGenerator& eventGenerator;
    std::function<Event(Producer&)> eventHandler;
    std::deque<Event> eventQueue;
    MutexLocker queueLocker;
    std::atomic<bool> running{true};
    std::condition_variable cv;
    std::mutex cvMutex;
    std::future<void> detectionFuture;
    std::future<void> notificationFuture;

    void detectionThreadFunction() {
        try {
            while (running) {
                Event event = eventHandler(*this);
                queueLocker.runWithLockGuard([&]() {
                    eventQueue.push_back(event);
                });
                cv.notify_one();
            }
        } catch (const std::exception& ex) {
            log("Error in detection thread: " + std::string(ex.what()));
        }
    }

    void notificationThreadFunction() {
        try {
            while (running || !eventQueue.empty()) {
                std::unique_lock<std::mutex> lock(cvMutex);
                cv.wait(lock, [this] { return !eventQueue.empty() || !running; });

                if (!eventQueue.empty()) {
                    Event event;
                    queueLocker.runWithLockGuard([&]() {
                        event = eventQueue.front();
                        eventQueue.pop_front();
                    });
                    notifyObservers(event);
                }
            }
        } catch (const std::exception& ex) {
            log("Error in notification thread: " + std::string(ex.what()));
        }
    }

public:
    Producer(int id, EventIdGenerator& generator, std::function<Event(Producer&)> handler)
        : id(id), eventGenerator(generator), eventHandler(std::move(handler)) {}

    void produceAsync() {
        detectionFuture = std::async(std::launch::async, &Producer::detectionThreadFunction, this);
        notificationFuture = std::async(std::launch::async, &Producer::notificationThreadFunction, this);
    }

    void stop() {
        running = false;
        cv.notify_all();
        if (detectionFuture.valid()) detectionFuture.get();
        if (notificationFuture.valid()) notificationFuture.get();
        log("Producer " + std::to_string(id) + " stopped.");
    }

    Event createEvent(const std::string& topic, const std::string& data) {
        return { id, topic, eventGenerator.getNextEventId(topic), data };
    }
};

/**
 * ProducerConsumerOrchestrator manages producers and consumers for event processing.
 */
class ProducerConsumerOrchestrator {
private:
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::function<void()>> configurations;
    EventIdGenerator eventGenerator;

public:

    ProducerConsumerOrchestrator& configure(const std::function<void(ProducerConsumerOrchestrator&)>& config) {
        configurations.push_back([this, config] { config(*this); });
        return *this;
    }

    std::shared_ptr<Producer> createProducer(int id, std::function<Event(Producer&)> logic) {
        auto producer = std::make_shared<Producer>(id, eventGenerator, std::move(logic));
        producers.push_back(producer);
        return producer;
    }

    std::shared_ptr<Consumer> createConsumer(std::function<void(const Event&)> logic) {
        auto consumer = std::make_shared<Consumer>(consumers.size() + 1, std::move(logic));
        consumers.push_back(consumer);
        return consumer;
    }

    void produceAndConsumeEvents() {
        log("Apply configurations before starting the producers.");
        for (const auto& config : configurations) {
            config();
        }

        log("Starting producers...");
        for (auto& producer : producers) {
            producer->produceAsync();
        }

        log("Simulation running...");
        std::this_thread::sleep_for(std::chrono::seconds(5));

        log("Stopping producers...");
        for (auto& producer : producers) {
            producer->stop();
        }
    }
};

/**
 * Main function demonstrates the Producer-Consumer system.
 */
int main() {
    ProducerConsumerOrchestrator orchestrator;

    orchestrator
        .configure([&](ProducerConsumerOrchestrator& orchestrator) {
            auto consumer1 = orchestrator.createConsumer([](const Event& event) {
                log("Custom handler for Consumer 1: " + event.changeData + " from Producer " + std::to_string(event.producerId) + " with ID " + std::to_string(event.eventId));
            });

            auto consumer2 = orchestrator.createConsumer([](const Event& event) {
                log("Custom handler for Consumer 2: " + event.changeData + " from Producer " + std::to_string(event.producerId) + " with ID " + std::to_string(event.eventId));
            });

            auto producer1 = orchestrator.createProducer(1, [&](Producer& producer) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                return producer.createEvent("topicA", "Custom event data for topic A");
            });

            producer1->addObserver("topicA", consumer1);
            producer1->addObserver("topicA", consumer2);

            auto producer2 = orchestrator.createProducer(2, [&](Producer& producer) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                return producer.createEvent("topicB", "Custom event data for topic B");
            });

            producer2->addObserver("topicB", consumer2);
        })
        .produceAndConsumeEvents();

    return 0;
}
