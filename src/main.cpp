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
    EventIdGenerator& eventGenerator; ///< Shared event ID generator.
    std::function<Event(Producer&)> dataGenerationHandler; ///< Custom data generation handler.
    std::deque<Event> eventQueue; ///< Queue for detected events.
    MutexLocker queueLocker; ///< MutexLocker for the event queue.
    std::atomic<bool> running{true}; ///< Indicates if the producer is running.
    std::condition_variable cv; ///< Condition variable for notification.
    std::mutex cvMutex; ///< Mutex for the condition variable.
    std::future<void> detectionFuture; ///< Future for detection thread.
    std::future<void> notificationFuture; ///< Future for notification thread.

    /**
     * Detects events and pushes them to the event queue.
     */
    void detectionThreadFunction() {
        while (running) {
            Event event = dataGenerationHandler(*this);
            log("Producer " + std::to_string(id) + " detected event: " + std::to_string(event.eventId) + " on topic " + event.topic);
            queueLocker.runWithLockGuard([&]() {
                eventQueue.push_back(event);
            });
            cv.notify_one();
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Avoid CPU overuse.
        }
    }

    /**
     * Notifies observers of events from the event queue.
     */
    void notificationThreadFunction() {
        while (running || !eventQueue.empty()) {
            std::unique_lock<std::mutex> lock(cvMutex);
            cv.wait(lock, [this] {
                return !eventQueue.empty() || !running;
            });

            if (!eventQueue.empty()) {
                queueLocker.runWithLockGuard([&]() {
                    Event event = eventQueue.front();
                    eventQueue.pop_front();
                    notifyObservers(event);
                });
            }
        }
    }

public:
    /**
     * Constructor initializes a producer with an ID, event generator, and custom logic.
     * @param id Unique producer ID.
     * @param generator Reference to the event ID generator.
     * @param logic Custom logic for generating event data.
     */
    explicit Producer(int id, EventIdGenerator& generator, std::function<Event(Producer&)> logic)
        : id(id), eventGenerator(generator), dataGenerationHandler(std::move(logic)) {}

    /**
     * Starts producing events asynchronously.
     * @return A reference to the current object.
     */
    Producer& produceAsync() {
        detectionFuture = std::async(std::launch::async, &Producer::detectionThreadFunction, this);
        notificationFuture = std::async(std::launch::async, &Producer::notificationThreadFunction, this);
        return *this;
    }

    Event createEvent(const std::string& topic, const std::string& data) {
        return Event{ id, topic, eventGenerator.getNextEventId(topic), data };
    }

    /**
     * Stops the producer from generating further events.
     */
    void stop() {
        running = false;
        cv.notify_all();
        if (detectionFuture.valid()) {
            detectionFuture.get();
        }
        if (notificationFuture.valid()) {
            notificationFuture.get();
        }
        log("Producer " + std::to_string(id) + " stop signal issued.");
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
    std::shared_ptr<Producer> createProducer(int id, std::function<Event(Producer&)> logic) {
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
        for (const auto& config : configurations) {
            // Apply configurations before starting the consumers.
            config();
        }

        std::vector<std::thread> threads;
        for (auto& consumer : consumers) {
            threads.emplace_back([consumer] { consumer->processEvents(); });
        }

        for (auto& producer : producers) {
            producer->produceAsync();
        }

        std::this_thread::sleep_for(std::chrono::seconds(5));

        for (auto& producer : producers) {
            producer->stop();
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

            auto producer1 = orchestrator.createProducer(1, [&](Producer& producer) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                return producer.createEvent("topicA", "Custom event data for topic A");
            });

            producer1->addObserver("topicA", consumer1);

            auto producer2 = orchestrator.createProducer(2, [&](Producer& producer) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                return producer.createEvent("topicA", "Custom event data for topic A");
            });

            producer2->addObserver("topicB", consumer2);

        })
        .consumeEvents();

    return 0;
}
