module.exports = {
    apps: [
        {
            name: "kafkaApiGateway",

            script: "bun",

            args: ["run", "start"],

            cwd: "/home/dev/Local-Disk-F/vs-code/kafka-api-gateway",

            exec_mode: "fork",
            instances: 1,

            windowsHide: true,
            autorestart: true,
            max_restarts: 3,
            restart_delay: 3000,

            env: {
                NODE_ENV: "production"
            }
        }
    ]
};
