# e621-noapi-cli

A tool for furry diffusioners that makes building e621 datasets easier.

-------------------

## Changelog

### 0.1 - Initial release

- This is the initial release version.

-------------------

## Package Features

- Command line tool to build e621 datasets
- Does not make e621 api calls (thanks to DBExport manager)
- Automatic image cropping with pillow and multires cropping
- Automatic image format unification.
- Automatic training dataset packaging for use in tools such as AUTOMATIC1111's webui.

-------------------

## Repository Features

- Github workflows support
- Code standardization with [black](https://github.com/psf/black)
- Code linting with [flake8](https://github.com/PyCQA/flake8)
- Code security testing with CodeQL Analysers

-------------------

## Why this?

Direct, does not stress e621 api, allows us to make our own tag queries for training in exact e621 search syntax, does 95% of the work and hard heavylifting of building an 512x512 image dataset with tags from danbooru boards for us for stable diffusion and embedding training.

-------------------

## License

Please visit [here](./LICENSE.md) for a copy of the repository's software license. 

-------------------

## Contributing

Please visit [here](./CONTRIBUTING.md) to review contributing guidelines.