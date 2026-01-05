# Instructions for Creating DIGEST and DELEX GitHub Issue

This directory contains a feature request document (`feature-request-digest-delex.md`) that should be used to create a GitHub issue requesting implementation of the DIGEST and DELEX commands.

## How to Create the Issue

1. Navigate to the GitHub repository: https://github.com/dragonflydb/dragonfly/issues/new/choose

2. Select "Feature request" from the issue templates

3. Use the following information from `feature-request-digest-delex.md`:

   **Title:**
   ```
   Implement DIGEST and DELEX commands for atomic conditional operations
   ```

   **Body:** Copy the relevant sections from the feature request document:
   - Summary
   - Description (both DIGEST and DELEX sections)
   - Benefits
   - Implementation Considerations (optional, can be summarized)
   - Additional Context
   - References

4. Apply labels:
   - `feature request` (should be auto-applied)
   - `redis-compatibility` (if available)
   - `commands` (if available)

## Key Points to Include

- Clear description of what DIGEST and DELEX commands do
- Their use cases and benefits
- References to official Redis documentation
- Implementation considerations for Dragonfly maintainers
- Note about compatibility with latest Redis releases (without mentioning specific version)

## Note

The feature request document does NOT mention "Redis 8.4" specifically, as per requirements. It refers to "latest Redis releases" and "latest Redis command set" instead.
