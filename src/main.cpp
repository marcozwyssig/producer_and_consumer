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

// Utility for logging with thread ID
void log(const std::string& message) {
    std::ostringstream oss;
    oss << "[Thread " << std::this_thread::get_id() << "] " << message << std::endl;
    std::cout << oss.str();
}

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
    std::mutex mtx;

public:
    Observable& addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        std::lock_guard<std::mutex> lock(mtx);
        topicObservers[topic].push_back(observer);
        log("Added observer to topic: " + topic);
        return *this;
    }

    void notifyObservers(const Event& event) {
        std::lock_guard<std::mutex> lock(mtx);
        log("Notifying observers for topic: " + event.topic + ", Event ID: " + std::to_string(event.eventId));
        if (topicObservers.find(event.topic) != topicObservers.end()) {
            for (auto& observer : topicObservers[event.topic]) {
                if (observer) {
                    observer->update(event);
                }
            }
        }
    }
};

// Concrete Observer (Consumer)
class Consumer : public Observer {
private:
    int id; // Unique consumer ID
    std::queue<Event> eventQueue;
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> running{true};

public:
    explicit Consumer(int id) : id(id) {
        log("Consumer " + std::to_string(id) + " created.");
    }

    void update(const Event& event) override {
        {
            std::lock_guard<std::mutex> lock(mtx);
            eventQueue.push(event);
            log("Consumer " + std::to_string(id) + " received event: Producer " +
                std::to_string(event.producerId) + ", Topic " + event.topic +
                ", Event " + std::to_string(event.eventId));
        }
        cv.notify_one();
    }

    void processEvents() {
        log("Consumer " + std::to_string(id) + " started processing events.");
        while (running) {
            Event event;
            {
                std::unique_lock<std::mutex> lock(mtx);
                cv.wait(lock, [this] { return !eventQueue.empty() || !running; });

                if (!running && eventQueue.empty()) {
                    break;
                }

                event = eventQueue.front();
                eventQueue.pop();
            }
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
    std::unordered_map<std::string, int> topicCounters; // Tracks the last event ID per topic
    std::mutex mtx; // Mutex for thread-safe counter updates

public:
    explicit Producer(int id) : id(id) {}

    Producer& addObserver(const std::string& topic, const std::shared_ptr<Observer>& observer) {
        Observable::addObserver(topic, observer);
        return *this;
    }

    void produceAsync(const std::string& topic, int numEvents) {
        producerFuture = std::async(std::launch::async, [this, topic, numEvents]() {
            for (int i = 0; i < numEvents; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Simulate work
                
                int eventId;
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    eventId = ++topicCounters[topic]; // Increment and get unique event ID for the topic
                }

                Event event{id, topic, eventId, "Generated event " + std::to_string(eventId)};
                log("Producer " + std::to_string(id) + " generated event: " +
                    std::to_string(eventId) + " on topic " + topic);
                notifyObservers(event);
            }
        });
    }

    void wait() {
        if (producerFuture.valid()) {
            producerFuture.get();
        }
        log("Producer " + std::to_string(id) + " finished producing.");
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
        log("Created Consumer with ID: " + std::to_string(consumerCounter));
        return consumer;
    }

    Producer& createProducer(int id) {
        auto producer = std::make_shared<Producer>(id);
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

    ProducerConsumerOrchestrator& startProducers(const std::string& topic, int numEvents) {
        log("Starting producers for topic: " + topic + " with " + std::to_string(numEvents) + " events.");
        for (auto& producer : producers) {
            producer->produceAsync(topic, numEvents);
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
    ProducerConsumerOrchestrator orchestrator;

    auto consumer1 = orchestrator.createConsumer(); // Consumer 1
    auto consumer2 = orchestrator.createConsumer(); // Consumer 2

    orchestrator.createProducer(1)
        .addObserver("topicA", consumer1)
        .addObserver("topicB", consumer1)
        .addObserver("topicA", consumer2)
        .addObserver("topicB", consumer2);

    orchestrator.createProducer(2)
        .addObserver("topicA", consumer1)
        .addObserver("topicB", consumer2);

    orchestrator
        .startConsumers()
        .startProducers("topicA", 2)
        .startProducers("topicA", 2)
        .waitForProducers()
        .stopConsumers();

    log("Producer-consumer orchestrator finished.");
    return 0;
}
