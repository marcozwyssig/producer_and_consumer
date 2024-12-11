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
#include <optional>
#include <functional>

/**
 * Utility for logging messages with thread ID for debugging purposes.
 * @param message The message to log.
 */
void log(const std::string& message) {
    std::ostringstream oss;
    oss << "[Thread " << std::this_thread::get_id() << "] " << message << std::endl;
    std::cout << oss.str();
}

/**
 * MutexLocker class encapsulates mutex functionality for thread safety.
 */
class MutexLocker {
public:
    MutexLocker() = default; ///< Default constructor.

    /**
     * Executes a task within a lock_guard to ensure thread safety.
     * @param task A callable task to execute.
     * @return The result of the task.
     */
    template <typename Callable>
    auto runWithLockGuard(Callable&& task) {
        std::lock_guard<std::mutex> lock(mtx);
        return task();
    }

    /**
     * Executes a task with a unique_lock, allowing condition variable usage.
     * @param task A callable task to execute.
     * @return The result of the task.
     */
    template <typename Callable>
    auto runWithUniqueLock(Callable&& task) {
        std::unique_lock<std::mutex> lock(mtx);
        return task(std::move(lock));
    }

private:
    std::mutex mtx; ///< Internal mutex.
};

/**
 * Event structure represents a producer-generated event.
 */
struct Event {
    int producerId;        ///< ID of the producer generating the event.
    std::string topic;     ///< Topic of the event.
    int eventId;           ///< Unique event ID.
    std::string changeData; ///< Data associated with the event.
};

/**
 * Observer interface for receiving event updates.
 */
class Observer {
public:
    virtual void update(const Event& event) = 0; ///< Called when an event is received.
    virtual ~Observer() = default; ///< Virtual destructor.
};

/**
 * Observable class manages topic-specific observers and event notifications.
 */
class Observable {
private:
    std::unordered_map<std::string, std::vector<std::shared_ptr<Observer>>> topicObservers; ///< Observer map.
    MutexLocker locker; ///< MutexLocker for thread safety.

public:
    /**
     * Adds an observer to a specific topic.
     * @param topic The topic to observe.
     * @param observer The observer to add.
     * @return A reference to the current object.
     */
    Observable& addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        locker.runWithLockGuard([&] {
            topicObservers[topic].push_back(observer);
            log("Added observer to topic: " + topic);
        });
        return *this;
    }

    /**
     * Notifies observers about an event.
     * @param event The event to notify.
     */
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

/**
 * EventIdGenerator generates unique IDs for events per topic.
 */
class EventIdGenerator {
private:
    std::unordered_map<std::string, int> topicEventCounters; ///< Counters for each topic.
    MutexLocker locker; ///< MutexLocker for thread safety.

public:
    /**
     * Gets the next unique event ID for a topic.
     * @param topic The topic for which to generate an ID.
     * @return The next unique event ID.
     */
    int getNextEventId(const std::string& topic) {
        return locker.runWithLockGuard([&] {
            int& counter = topicEventCounters[topic];
            return ++counter;
        });
    }
};

/**
 * Consumer class represents an observer that processes events.
 */
class Consumer : public Observer {
private:
    int id; ///< Unique consumer ID.
    std::queue<Event> eventQueue; ///< Queue of received events.
    MutexLocker locker; ///< MutexLocker for thread safety.
    std::condition_variable cv; ///< Condition variable for event synchronization.
    std::atomic<bool> running{true}; ///< Indicates if the consumer is running.
    std::function<void(const Event&)> consumeLogic; ///< Custom consume logic.

public:
    /**
     * Constructor initializes a consumer with an ID and custom logic.
     * @param id Unique consumer ID.
     * @param logic Custom logic for processing events.
     */
    explicit Consumer(int id, std::function<void(const Event&)> logic) 
        : id(id), consumeLogic(std::move(logic)) {
        log("Consumer " + std::to_string(id) + " created.");
    }

    /**
     * Called when an event is received from the producer.
     * @param event The event received.
     */
    void update(const Event& event) override {
        locker.runWithLockGuard([&] {
            eventQueue.push(event);
            log("Consumer " + std::to_string(id) + " received event: Producer " +
                std::to_string(event.producerId) + ", Topic " + event.topic +
                ", Event " + std::to_string(event.eventId));
        });
        cv.notify_one();
    }

    /**
     * Processes events in the queue until stopped.
     */
    void processEvents() {
        while (running) {
            std::optional<Event> eventOpt = locker.runWithUniqueLock([&](std::unique_lock<std::mutex> lock) {
                cv.wait(lock, [this] {
                    return !eventQueue.empty() || !running;
                });
                if (!running && eventQueue.empty()) {
                    return std::optional<Event>{};
                }
                Event event = eventQueue.front();
                eventQueue.pop();
                return std::optional<Event>{event};
            });

            if (!eventOpt.has_value()) {
                break;
            }

            const auto& event = eventOpt.value();
            consumeLogic(event);
        }
        log("Consumer " + std::to_string(id) + " stopped processing events.");
    }

    /**
     * Stops the consumer from processing further events.
     */
    void stop() {
        running = false;
        cv.notify_all();
        log("Consumer " + std::to_string(id) + " stop signal issued.");
    }
};

/**
 * Producer class generates events and notifies observers.
 */
class Producer : public Observable {
private:
    int id; ///< Unique producer ID.
    std::future<void> producerFuture; ///< Future for asynchronous event generation.
    EventIdGenerator& eventGenerator; ///< Shared event ID generator.
    std::function<std::string(int, const std::string&)> dataGenerationLogic; ///< Custom data generation logic.

public:
    /**
     * Constructor initializes a producer with an ID, event generator, and custom logic.
     * @param id Unique producer ID.
     * @param generator Reference to the event ID generator.
     * @param logic Custom logic for generating event data.
     */
    explicit Producer(int id, EventIdGenerator& generator, std::function<std::string(int, const std::string&)> logic) 
        : id(id), eventGenerator(generator), dataGenerationLogic(std::move(logic)) {}

    /**
     * Adds an observer to a topic.
     * @param topic The topic to observe.
     * @param observer The observer to add.
     * @return A reference to the current object.
     */
    Producer& addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        Observable::addObserver(topic, observer);
        return *this;
    }

    /**
     * Produces events asynchronously for a topic.
     * @param topic The topic to produce events for.
     * @param numEvents The number of events to generate.
     * @return A reference to the current object.
     */
    Producer& produceAsync(const std::string& topic, int numEvents) {
        producerFuture = std::async(std::launch::async, [this, topic, numEvents]() {
            for (int i = 0; i < numEvents; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                int eventId = eventGenerator.getNextEventId(topic);
                std::string data = dataGenerationLogic(id, topic);
                Event event{id, topic, eventId, data};
                log("Producer " + std::to_string(id) + " generated event: " +
                    std::to_string(event.eventId) + " on topic " + topic);
                notifyObservers(event);
            }
        });
        return *this;
    }

    /**
     * Waits for the producer to finish generating events.
     */
    void wait() {
        if (producerFuture.valid()) {
            producerFuture.get();
        }
        log("Producer " + std::to_string(id) + " finished producing.");
    }
};

/**
 * ProducerConsumerOrchestrator manages producers and consumers for event processing.
 */
class ProducerConsumerOrchestrator {
private:
    std::vector<std::shared_ptr<Consumer>> consumers; ///< List of consumers.
    std::vector<std::shared_ptr<Producer>> producers; ///< List of producers.
    std::vector<std::function<void()>> configurations; ///< Configurations to apply.
    EventIdGenerator eventGenerator; ///< Integrated event ID generator.

public:
    /**
     * Configures and initiates event production.
     * @param config A function to configure the orchestrator.
     * @return A reference to the current object.
     */
    ProducerConsumerOrchestrator& configureAndProduceEvents(const std::function<void(ProducerConsumerOrchestrator&)>& config) {
        configurations.push_back([this, config] { config(*this); });
        return *this;
    }

    /**
     * Creates a producer with a unique ID and custom logic.
     * @param id The unique ID for the producer.
     * @param logic Custom logic for generating event data.
     * @return A shared pointer to the created producer.
     */
    std::shared_ptr<Producer> createProducer(int id, std::function<std::string(int, const std::string&)> logic) {
        auto producer = std::make_shared<Producer>(id, eventGenerator, std::move(logic));
        producers.push_back(producer);
        return producer;
    }

    /**
     * Creates a consumer with custom logic.
     * @param logic Custom logic for processing events.
     * @return A shared pointer to the created consumer.
     */
    std::shared_ptr<Consumer> createConsumer(std::function<void(const Event&)> logic) {
        auto consumer = std::make_shared<Consumer>(consumers.size() + 1, std::move(logic));
        consumers.push_back(consumer);
        return consumer;
    }

    /**
     * Starts consuming and processing events.
     * @return A reference to the current object.
     */
    ProducerConsumerOrchestrator& consumeEvents() {
        for (const auto& config : configurations) config();

        std::vector<std::thread> threads;
        for (auto& consumer : consumers) {
            threads.emplace_back([consumer] { consumer->processEvents(); });
        }

        for (auto& producer : producers) {
            producer->wait();
        }

        for (auto& consumer : consumers) {
            consumer->stop();
        }

        for (auto& thread : threads) {
            if (thread.joinable()) thread.join();
        }

        return *this;
    }
};

/**
 * Main function demonstrates the Producer-Consumer system.
 */
int main() {
    ProducerConsumerOrchestrator orchestrator;

    orchestrator
        .configureAndProduceEvents([&](ProducerConsumerOrchestrator& orchestrator) {
            auto consumer1 = orchestrator.createConsumer([](const Event& event) {
                log("Custom handler for Consumer 1: " + event.changeData);
            });

            auto consumer2 = orchestrator.createConsumer([](const Event& event) {
                log("Custom handler for Consumer 2: Processing " + event.changeData);
            });

            auto producer1 = orchestrator.createProducer(1, [](int producerId, const std::string& topic) {
                return "Custom event data for topic " + topic + " by Producer " + std::to_string(producerId);
            });

            producer1->addObserver("topicA", consumer1);

            auto producer2 = orchestrator.createProducer(2, [](int producerId, const std::string& topic) {
                return "Generated data for topic " + topic;
            });

            producer2->addObserver("topicB", consumer2);

            producer1->produceAsync("topicA", 1);
            producer2->produceAsync("topicB", 1);
        })
        .consumeEvents();

    return 0;
}
