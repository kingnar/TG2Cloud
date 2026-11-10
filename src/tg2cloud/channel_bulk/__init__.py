"""Admin channel inventory and paused bulk transfer.

The package does not open a Telegram session. Callers pass the listener's
existing client, and jobs stay in preview until an administrator resumes them.
"""
