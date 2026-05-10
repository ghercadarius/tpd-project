# Federated Learning: Document Summary

## 1. Introduction and Core Concepts
* **Definition**: Federated Learning (FL) is a machine learning setting where multiple clients collaborate to solve a learning problem under the coordination of a central server or service provider.
* **Data Privacy**: A defining characteristic of FL is that raw data is generated and stored locally on the client's device (e.g., mobile phones, wearable devices, IoT). The data is never transferred or seen by the central server or other clients. 
* **Benefits**: Training models at the edge using FL reduces strain on network infrastructure, incorporates new localized data, and significantly improves user privacy.

## 2. Collaborative Learning vs. Federated Learning
* While both involve multiple parties training together, they differ in the tightness of collaboration and data sharing.
* **Collaborative Learning**: Parties may jointly train models by explicitly sharing data, features, gradients, or intermediate representations. It often uses a peer-to-peer structure.
* **Federated Learning**: Relies on a central orchestration server, but the data remains decentralized and is never shared. The clients only send focused model updates intended for immediate aggregation.

## 3. Types of Federated Learning architectures
* **Cross-Device FL**: 
  * Involves millions of intermittently available client devices.
  * The server only accesses a random, possibly biased sample of clients in each round.
  * Most clients only participate once.
  * Network communication is typically the primary bottleneck.
* **Cross-Silo FL**: 
  * Involves a small number of high-availability clients (like institutions or data silos).
  * Most clients participate in every round.
  * Clients can run algorithms that maintain local state across rounds.
  * Either communication or computation can be the primary bottleneck.

## 4. Real-World Applications (e.g., Gboard)
* **Next-Word Prediction**: Using Federated Recurrent Neural Networks (RNNs) yielded 24% better prediction accuracy and increased useful prediction clicks by 10%.
* **Emoji and Action Prediction**: Produced more accurate emoji predictions and a 47% reduction in unhelpful action suggestions (e.g., GIFs, stickers).
* **Discovering New Words**: Enabled the federated discovery of new words typed by users that were previously out-of-vocabulary for the model.

## 5. Federated Averaging (FedAvg) Algorithm
* **FedAvg** is the baseline algorithm in federated learning used to train a shared global model without moving data off devices.
* **Process**: In each communication round, a subset of devices runs Stochastic Gradient Descent (SGD) locally. The server then averages these local model updates.
* **Advantage over Distributed SGD**: By performing more local computation (local-updating) before communicating, FedAvg can reduce the total number of communication rounds by roughly 100x compared to traditional distributed SGD.

## 6. Key Challenges in Federated Learning
* **Expensive Communication**: Involves massive, slow, and unreliable networks.
* **Privacy Concerns**: Requires adherence to strict user privacy constraints (often managed by combining FL with additional privacy mechanisms).
* **Systems Heterogeneity**: Devices have variable hardware capabilities, battery levels, and connectivity. Dropping slow devices can exacerbate model convergence issues.
* **Statistical Heterogeneity**: Data across devices is highly unbalanced and non-identically distributed (non-IID), which can bias optimization procedures and cause client models to drift far from the global model.

## 7. Approaches to Tackle Heterogeneity
* **FedProx**: Modifies FedAvg by adding a proximal term to the objective function, which limits the impact of heterogeneous local updates and safely incorporates partial work from slow devices.
* **Personalized Models**: Rather than forcing a single global model on all heterogeneous clients, personalization learns both a shared global component and a client-specific local component. Techniques include:
  * *Model Interpolation*: Combining global and local models.
  * *Fine-tuning*: Adapting the global model to local data.
  * *Multi-task Learning*: Jointly learning shared yet personalized models where each client is treated as a separate, related task.
* **Clustering Clients**: Grouping clients with similar data distributions.

## 8. Federated Learning Systems and Frameworks
* **Flower**: An open-source framework supporting PyTorch, TensorFlow, etc., allowing for simulation and real deployments with built-in strategies (like FedAvg).
* **TensorFlow Federated**: Google's research-focused framework tightly integrated with TensorFlow.
* **FedML**: A hybrid research and production framework that scales to distributed clusters and includes benchmarking tools.
