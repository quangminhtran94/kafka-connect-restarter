**Kafka Connect restarter**

Sometimes failed kafka connector tasks can be resolved by restarting, but it needs human intervention.
This small program is to automatically restart failed tasks periodically

Future feature:
1. Filter connectors to restart by regex
2. Notification when restart failed tasks
3. Upper limit to number of restart time in a duration of time 