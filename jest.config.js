module.exports = {
  transform: {
    "\\.ts$": "esbuild-runner/jest",
  },
  setupFilesAfterEnv: ["./test/setup.js"],
};
