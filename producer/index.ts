import { createClient } from "redis";
import prismaClient from "../../avadhi/packages/store/generated/prisma/client";

async function main() {
    try {
        const websites = await prismaClient.website.findMany();

        const client = createClient();
        client.on("error", (err) => console.log("Redis Client Error", err));
        await client.connect();

        for (const website of websites) {
            await client.xAdd(
                "betteruptime:website",
                "*",
                {
                    url: website.url,
                    website_id: website.id,
                }
            );
        }

        await client.quit();
        console.log(`Pushed ${websites.length} websites to the stream.`);
    } catch (err) {
        console.error("Producer error:", err);
    }
}

// Run every 3 minutes
setInterval(main, 3 * 60 * 1000);

// Optionally, run once at startup
main();
