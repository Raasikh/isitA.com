# isitA.com 

### Environment Setup

Here’s a concise **step-by-step guide** for someone to run the application - only the essential steps:

---
> ### Hey Bro
> - I noticed we have many commands that we will likely repeat often, so I decided to use `make`. If you are using Linux or Mac, `make` is available by default. If you are on Windows, check [this guide](https://medium.com/@AliMasaoodi/installing-make-on-windows-10-using-chocolatey-a-step-by-step-guide-5e178c449394). Let me know if you run into any issues. 
> - Remember to create the .env file you can copy the example file that I pushed.
> - I refactored the way Airflow is installed because I also wanted to install other images, e.g., Postgres for pgvector. I avoided using the Postgres version for Airflow since there are already many dedicated tables, and our project could become more complex, which might cause confusion.  
> - I’m using Alembic for database version control. This ensures our database tables are always synchronized. After pulling the code, you can simply run `make migrate`, and it will apply any database changes I created or updated.  
> - I upgraded Airflow to version 3 and created a custom Docker image to make installing dependencies easier.  
> - I added a few endpoints for embeddings. By default, if `APP_ENV` is not set to `production`, it will use `FakeEmbeddings`, but they will still be populated in the database.  
> - I installed `crawl4ai` in the Airflow environment. Feel free to test it and ensure everything works as expected.  
> - When embedding data, the document endpoints currently support the following formats:  
>   - PDF files (`.pdf`)  
>   - Plain text files (`.txt`)  
>   - HTML files (`.html`)  
>   - Microsoft Word files (`.doc` and `.docx`)  
>   I can add support for additional formats if needed.  
> - Please note that the first Docker build will be a bit slow, but subsequent builds will be faster. This is because Playwright, a major dependency of `crawl4ai`, takes time to install.
> - Connect to the postgres instance on 5432. Remember we have to add the pg vector extension and run the query ```sql CREATE EXTENSION IF NOT EXISTS vector;``` and run on terminal (you should activate the .venv) `make migrate`
> - To see all the endpoints ypu can use swagger check : (http://localhost:8005/docs)[http://localhost:8005/docs] . They should be 11 in total.



1. **Sync Dependencies**

```bash
make setup
```

2. **Start All Services**

```bash
make start
```

3. **Run Database Migrations**

```bash
make migrate
```

4. **Check Health (optional)**

```bash
make health
```

5. **View Logs (optional)**

```bash
make logs
```
6. **See all commands**

```bash
make help
```

7. **Stop Services (when done)**

```bash
make stop
```
