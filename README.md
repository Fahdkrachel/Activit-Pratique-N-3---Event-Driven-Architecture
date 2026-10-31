# Event-Driven Architecture : Kafka, Spring Cloud Stream & Kafka Streams

Activité pratique n°3 : application **Spring Boot** illustrant une architecture orientée événements. Elle produit, consomme et analyse en temps réel des événements de visite de pages (`PageEvent`) avec **Apache Kafka**, **Spring Cloud Stream** et **Kafka Streams**. Les résultats sont affichés dans un graphique en direct.

---

## Sommaire

1. [Fonctionnalités](#fonctionnalités)
2. [Architecture](#architecture)
3. [Stack technique](#stack-technique)
4. [Structure du projet](#structure-du-projet)
5. [Prérequis](#prérequis)
6. [Démarrage rapide](#démarrage-rapide)
7. [Utilisation](#utilisation)
8. [Configuration](#configuration)
9. [Commandes Kafka utiles](#commandes-kafka-utiles)
10. [Tests](#tests)
11. [Dépannage](#dépannage)
12. [Pistes d'amélioration](#pistes-damélioration)

---

## Fonctionnalités

- **Publication à la demande** d'un événement vers un topic via une API REST (`StreamBridge`).
- **Consommation** d'événements Kafka avec une fonction `Consumer`.
- **Production automatique** d'événements aléatoires avec des `Supplier` (polling toutes les 200 ms).
- **Traitement en flux** avec Kafka Streams : comptage des événements par page sur une fenêtre glissante de 5 secondes.
- **Interactive Queries** : lecture du state store `count-store` depuis l'API.
- **Dashboard temps réel** : flux SSE (`/analytics`) affiché avec Smoothie.js.
- **Monitoring** via Spring Boot Actuator.

## Architecture

```
                        ┌──────────────────────────────┐
 GET /publish ─────────►│ StreamBridge                 │
                        └──────────────┬───────────────┘
                                       ▼
                                      [T2] ───► pageEventConsumer (log console)

 pageEventSupplier (200 ms) ─────────► [T3] ───► kStreamFunction ───► [T4]
                                                  │ map → groupByKey
                                                  │ fenêtre 5 s → count
                                                  ▼
                                            "count-store"
                                                  │ Interactive Queries
 Navigateur ◄──── SSE ──── GET /analytics ◄───────┘

 fluxSupplier ─────────────────────────► [T11]
```

| Topic | Producteur | Consommateur |
|-------|-----------|--------------|
| `T2`  | `/publish` (StreamBridge) | `pageEventConsumer` |
| `T3`  | `pageEventSupplier` | `kStreamFunction` |
| `T4`  | `kStreamFunction` (clé : page, valeur : compteur) | console / client externe |
| `T11` | `fluxSupplier` | aucun (disponible pour extension) |

### Modèle d'événement

```java
public record PageEvent(String name, String user, Date date, long duration) {}
```

## Stack technique

| Composant | Version / détail |
|-----------|------------------|
| Java | 21 |
| Spring Boot | 4.1.2 |
| Spring Cloud | 2025.1.4 |
| Spring Cloud Stream | binders `kafka` et `kafka-streams` |
| Apache Kafka | Confluent Platform 7.3.0 (Docker) |
| Front | HTML + [Smoothie.js](http://smoothiecharts.org/) |
| Build | Maven (wrapper inclus) |

## Structure du projet

```
.
├── compose.yml                 # Zookeeper + broker Kafka
├── commands.txt                # Commandes Docker / Kafka de test
├── pom.xml
└── src
    ├── main
    │   ├── java/com/krachelfahd/kafkastreamsspringcloudstream
    │   │   ├── KafkaStreamsSpringCloudStreamApplication.java
    │   │   ├── controllers/PageEventController.java   # /publish et /analytics
    │   │   ├── handlers/PageEventHandler.java         # Consumer, Suppliers, KStream
    │   │   └── events/PageEvent.java                  # Record de l'événement
    │   └── resources
    │       ├── application.properties
    │       └── static/index.html                      # Dashboard temps réel
    └── test/.../KafkaStreamsSpringCloudStreamApplicationTests.java
```

## Prérequis

- JDK 21
- Docker et Docker Compose
- Maven (optionnel : le wrapper `mvnw` est fourni)
- Un navigateur avec accès internet (Smoothie.js est chargé depuis un CDN)

## Démarrage rapide

```bash
# 1. Cloner le dépôt
git clone <url-du-depot>
cd Activit-Pratique-N-3---Event-Driven-Architecture

# 2. Démarrer Kafka
docker compose up -d

# 3. Lancer l'application
./mvnw spring-boot:run        # Windows : mvnw.cmd spring-boot:run
```

L'application démarre sur **http://localhost:8080**. Le broker est exposé sur `localhost:9092`.

## Utilisation

### Dashboard temps réel

Ouvrir **http://localhost:8080** : le graphique affiche le nombre d'événements par page (P1 en vert, P2 en rouge), rafraîchi chaque seconde.

### Publier un événement

```bash
curl "http://localhost:8080/publish?name=P1&topic=T2"
```

Réponse (exemple) :

```json
{"name":"P1","user":"U1","date":"2026-10-31T13:35:00.000+00:00","duration":4821}
```

L'événement apparaît dans la console de l'application, via `pageEventConsumer`.

### Flux d'analytique (SSE)

```bash
curl -N http://localhost:8080/analytics
```

### Actuator

```
http://localhost:8080/actuator
http://localhost:8080/actuator/health
```

## Configuration

Extrait de `application.properties` :

```properties
# Fonctions Spring Cloud Function activées
spring.cloud.function.definition=pageEventConsumer;pageEventSupplier;fluxSupplier;kStreamFunction

# Bindings
spring.cloud.stream.bindings.pageEventConsumer-in-0.destination=T2
spring.cloud.stream.bindings.pageEventSupplier-out-0.destination=T3
spring.cloud.stream.bindings.fluxSupplier-out-0.destination=T11
spring.cloud.stream.bindings.kStreamFunction-in-0.destination=T3
spring.cloud.stream.bindings.kStreamFunction-out-0.destination=T4

# Fréquence du supplier et commit Kafka Streams
spring.cloud.stream.bindings.pageEventSupplier-out-0.producer.poller.fixed-delay=200
spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=5000
spring.cloud.stream.kafka.streams.binder.functions.kStreamFunction.applicationId=page-event-stream
```

> **Important :** `kStreamFunction` doit figurer dans `spring.cloud.function.definition`, sinon la topologie Kafka Streams n'est pas démarrée et le store `count-store` n'existe pas.

## Commandes Kafka utiles

```bash
# Lister les topics
docker exec -it broker kafka-topics --bootstrap-server broker:29092 --list

# Produire manuellement sur T2
docker exec -it broker kafka-console-producer --bootstrap-server broker:29092 --topic T2

# Consommer T3 (événements générés)
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic T3

# Consommer T4 (compteurs par page)
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic T4 \
  --property print.key=true --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

## Tests

```bash
./mvnw test
```

Le test `contextLoads` démarre le contexte Spring complet : le broker Kafka doit être lancé (`docker compose up -d`).

## Dépannage

| Symptôme | Cause probable | Solution |
|----------|----------------|----------|
| `/analytics` renvoie une erreur ou un flux vide | `kStreamFunction` non activée, ou aucun événement reçu | Vérifier `spring.cloud.function.definition` et les logs `=======>` |
| `Connection refused` vers `localhost:9092` | Kafka non démarré | `docker compose up -d` puis `docker ps` |
| Le graphique reste vide | CDN Smoothie.js inaccessible | Vérifier la connexion ou héberger le script localement |
| `contextLoads` échoue | Pas de broker disponible | Démarrer Kafka avant les tests |



