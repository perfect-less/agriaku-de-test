# AgriAku Data Engineer Test Answer
This repository contains my answer for AgriAku Data Engineer application test.

# Setup

1. Clone this repository to your local machine and `cd` into it.
```bash
$ git clone https://github.com/perfect-less/agriaku-de-test
$ cd agriaku-de-test
```

2. Create a new virtual environment. I did mine using python's `venv` although `conda` should also work just fine, although please remember to also install pip in your conda's virtual environment since we will install the dependencies using `pip`.
```bash
$ python3 -m venv venv
```

3. Activate the newly created virtual environment.
```bash
$ source venv/bin/activate
```

5. Install dependencies using `pip`
```bash
$ pip3 install -r requirements.txt
```


# Run

Please make sure that you are inside the repository and have already activate the environment before running the pipeline. Setup and installation step can be found above.

To run the pipeline just use the following command.
```bash
$ python3 run_pipeline.py
```
