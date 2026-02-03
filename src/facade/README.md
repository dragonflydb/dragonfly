## A facade library

The library is responsible for opening dragonfly-like TCP client connections.
I call it facade because "client" term is often abused.

It should be separated from the rest of dragonfly server logic and should be self-contained, i.e
no redis-lib or server dependencies are allowed.

