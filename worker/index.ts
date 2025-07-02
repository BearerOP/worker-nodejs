import { createClient } from "redis";
import axios from "axios";
import prismaClient from "../../avadhi/packages/store/generated/prisma/client";

async function main() {
    const client = createClient();
    client.on("error", (err) => console.log("Redis Client Error", err));
    await client.connect();

    const GROUP = "usa";
    const CONSUMER = "us-1";
    const STREAM = "betteruptime:website";
    const REGION_ID = "usa"; // or "india" for India worker

    while (true) {
        try {
            const res = await client.xReadGroup(
                GROUP,
                CONSUMER,
                { key: STREAM, id: ">" },
                { COUNT: 2, BLOCK: 5000 }
            );

            if (!res) {
                continue; // No messages, continue loop
            }

            for (const stream of res as any) {
                for (const message of stream.messages) {
                    // message.id, message.message (object of fields)
                    const { url, website_id } = message.message;
                    const startTime = Date.now();
                    try {
                        await axios.get(url);
                        await prismaClient.websiteTick.create({
                            data: {
                                status: "UP",
                                response_time_ms: Date.now() - startTime,
                                region_id: REGION_ID,
                                website_id: website_id,
                                status_code: 200,
                                status_text: "OK",
                                error_message: "",
                            },
                        });
                    } catch (err: any) {
                        await prismaClient.websiteTick.create({
                            data: {
                                status: "DOWN",
                                response_time_ms: Date.now() - startTime,
                                region_id: REGION_ID,
                                website_id: website_id,
                                status_code: err?.response?.status || 500,
                                status_text: err?.response?.statusText || "ERROR",
                                error_message: err?.message || "Unknown error",
                            },
                        });
                    }
                    // Acknowledge message
                    await client.xAck(STREAM, GROUP, message.id);
                }
            }
        } catch (err) {
            console.error("Worker error:", err);
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
    }
}

main();
