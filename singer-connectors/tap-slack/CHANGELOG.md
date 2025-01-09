1.1.1 (2023-02-28)
------------------
- Use `slack-sdk` instead of deprecated `slackclient` library.
- Increase page size when extracting user profiles from `users.list` API endpoint to circumvent rate limiting.

1.1.0 (2020-10-20)
------------------

- Extract user profiles from `users.list` API endpoint
- Extract message attachments from `conversations.history` API endpoint
- Fixed an issue when incremental bookmarks were not sent correctly in the `STATE` messages

1.0.1 (2020-10-02)
------------------

- Fixed an issue when `thread_ts` values were not populated correctly in `messages` and `threads` streams

1.0.0 (2020-09-30)
-------------------

- Initial release and fork of https://github.com/singer-io/tap-slack 1.0.0
