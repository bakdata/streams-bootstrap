# Render the documentation locally

## Prerequisites

- [just 1.5.0+](https://github.com/casey/just)
- [Poetry 1.8+](https://python-poetry.org/docs/#installation)

## Serve the docs

1. Clone the repository
   ```sh
   git clone git@github.com:bakdata/streams-bootstrap.git
   ```
2. Change directory to `streams-bootstrap/docs`.
3. Install the documentation dependencies and use `just` to serve the docs:
   ```sh
   just serve-docs
   ```
   Go to your browser and open [http://localhost:8000/](http://localhost:8000/).
