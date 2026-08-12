async function main() {
  const input = Bun.argv[2];

  if (!input) {
    console.error("Please provide an API key to hash.");
    console.error("Usage: bun run hash <api-key>");
    process.exit(1);
  }

  const hash = await Bun.password.hash(input, {
    algorithm: "bcrypt",
    cost: 12,
  });

  console.log(hash);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});