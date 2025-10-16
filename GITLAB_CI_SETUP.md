# GitLab CI/CD Configuration Guide

## 🔐 Required CI/CD Variables

To enable automatic deployment, you need to configure these variables in GitLab:

**Settings > CI/CD > Variables**

### Variables to Add

| Variable Name | Description | Example Value | Protected | Masked |
|--------------|-------------|---------------|-----------|---------|
| `DAGSTER_PG_PASSWORD` | Password for Dagster orchestration database | `SecurePassword123!` | ✅ Yes | ✅ Yes |
| `PG_PASSWORD` | Password for TimescaleDB/PostGIS databases | `SecurePassword456!` | ✅ Yes | ✅ Yes |
| `MINIO_USER` | MinIO admin username | `admin` | ✅ Yes | ❌ No |
| `MINIO_PASS` | MinIO admin password | `SecurePassword789!` | ✅ Yes | ✅ Yes |

### Step-by-Step Instructions

1. **Navigate to CI/CD Settings**
   - Go to your GitLab project
   - Click **Settings** (left sidebar)
   - Click **CI/CD**
   - Expand **Variables** section

2. **Add Each Variable**

   For each variable above:

   a. Click **Add variable** button

   b. Fill in the form:
      - **Key**: Use the exact name from table above (e.g., `DAGSTER_PG_PASSWORD`)
      - **Value**: Enter a strong password
      - **Type**: Variable (default)
      - **Environment scope**: All (default)
      - **Flags**:
        - ✅ **Protect variable**: Check this (only available on protected branches)
        - ✅ **Mask variable**: Check this for passwords (hides in logs)

   c. Click **Add variable**

3. **Verify Variables**

   After adding all 4 variables, you should see:
   ```
   DAGSTER_PG_PASSWORD  (Protected, Masked)
   PG_PASSWORD          (Protected, Masked)
   MINIO_USER           (Protected)
   MINIO_PASS           (Protected, Masked)
   ```

## 🚀 Testing the Pipeline

Once variables are configured:

1. **Push to main branch** (or create merge request)
   ```bash
   git push origin main
   ```

2. **Monitor pipeline**
   - Go to **CI/CD > Pipelines**
   - Click on the running pipeline
   - Watch the build and deploy stages

3. **Verify deployment**
   - Check the deploy job logs for errors
   - Visit your production URL (e.g., http://srv991054.hstgr.cloud:8080)
   - Dagster UI should load successfully

## 📋 Password Requirements

For production security, use strong passwords that:
- Are at least 16 characters long
- Include uppercase, lowercase, numbers, and symbols
- Are unique (don't reuse passwords)
- Are stored securely (use a password manager)

## 🔄 Updating Variables

To change a variable:
1. Go to **Settings > CI/CD > Variables**
2. Click the pencil icon (✏️) next to the variable
3. Update the value
4. Click **Update variable**
5. Redeploy by triggering a new pipeline

## ❓ Troubleshooting

### Pipeline fails with "variable not found"
- Check that variable names are EXACT (case-sensitive)
- Verify "Protected" flag is set for protected branches
- Ensure runner has access to project variables

### Deployment succeeds but services won't start
- Check docker-compose logs: `docker compose -f docker-compose.production.yml logs`
- Verify passwords don't contain special characters that need escaping
- Test credentials manually by connecting to services

### MinIO returns 403 Forbidden
- Verify `MINIO_USER` and `MINIO_PASS` match in GitLab variables
- Check MinIO is running: `docker ps | grep minio`
- Verify bucket was created: check MinIO console at port 9001

## 📚 Related Documentation

- [.env.template](../.env.template) - Local development credentials template
- [docker-compose.production.yml](../docker-compose.production.yml) - Production configuration
- [.gitlab-ci.yml](../.gitlab-ci.yml) - CI/CD pipeline definition
