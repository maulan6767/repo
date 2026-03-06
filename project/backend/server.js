const express = require('express');
const { Pool } = require('pg');
const Minio = require('minio');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const morgan = require('morgan');
const { body, validationResult } = require('express-validator');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Database configuration
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Test database connection
pool.connect((err, client, release) => {
  if (err) {
    return console.error('Error acquiring client', err.stack);
  }
  console.log('Database connected successfully');
  release();
});

// MinIO configuration
const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT,
  port: parseInt(process.env.MINIO_PORT),
  useSSL: false,
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
});

const bucketName = process.env.MINIO_BUCKET || 'user-profiles';

// Ensure bucket exists
async function ensureBucket() {
  try {
    const exists = await minioClient.bucketExists(bucketName);
    if (!exists) {
      await minioClient.makeBucket(bucketName, 'us-east-1');
      console.log(`Bucket ${bucketName} created successfully`);
      
      // Set bucket policy to public read
      const policy = {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Principal: { AWS: ['*'] },
            Action: ['s3:GetObject'],
            Resource: [`arn:aws:s3:::${bucketName}/*`],
          },
        ],
      };
      
      await minioClient.setBucketPolicy(bucketName, JSON.stringify(policy));
      console.log('Bucket policy set to public read');
    }
  } catch (error) {
    console.error('Error ensuring bucket:', error);
  }
}

ensureBucket();

// Configure multer for file upload
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadDir = './uploads';
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir);
    }
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, 'profile-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const fileFilter = (req, file, cb) => {
  const allowedTypes = /jpeg|jpg|png|gif/;
  const extname = allowedTypes.test(path.extname(file.originalname).toLowerCase());
  const mimetype = allowedTypes.test(file.mimetype);

  if (mimetype && extname) {
    return cb(null, true);
  } else {
    cb(new Error('Only image files are allowed'));
  }
};

const upload = multer({ 
  storage: storage,
  limits: { fileSize: 5 * 1024 * 1024 }, // 5MB limit
  fileFilter: fileFilter
});

// Validation rules
const validateUser = [
  body('name').notEmpty().withMessage('Name is required').trim().escape(),
  body('email').isEmail().withMessage('Valid email is required').normalizeEmail(),
];

// Helper function to upload to MinIO
async function uploadToMinIO(file, userId) {
  try {
    const fileName = `user-${userId}${path.extname(file.originalname)}`;
    const fileStream = fs.createReadStream(file.path);
    
    await minioClient.putObject(bucketName, fileName, fileStream, file.size);
    
    // Generate URL
    const publicUrl = `http://${process.env.MINIO_ENDPOINT}:${process.env.MINIO_PORT}/${bucketName}/${fileName}`;
    
    // Clean up temp file
    fs.unlinkSync(file.path);
    
    return {
      fileName,
      url: publicUrl
    };
  } catch (error) {
    console.error('Error uploading to MinIO:', error);
    throw error;
  }
}

// Routes

// Create user
app.post('/users', upload.single('photo'), validateUser, async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }

  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    const { name, email } = req.body;
    
    // Insert user
    const userResult = await client.query(
      'INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id',
      [name, email]
    );
    
    const userId = userResult.rows[0].id;
    let photoUrl = null;
    
    // Upload photo if exists
    if (req.file) {
      const photo = await uploadToMinIO(req.file, userId);
      photoUrl = photo.url;
      
      // Update user with photo URL
      await client.query(
        'UPDATE users SET photo_url = $1 WHERE id = $2',
        [photoUrl, userId]
      );
    }
    
    await client.query('COMMIT');
    
    res.status(201).json({
      id: userId,
      name,
      email,
      photo_url: photoUrl
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error creating user:', error);
    
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Email already exists' });
    }
    
    res.status(500).json({ error: 'Internal server error' });
  } finally {
    client.release();
  }
});

// Get all users
app.get('/users', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, name, email, photo_url, created_at FROM users ORDER BY created_at DESC'
    );
    
    // Transform URLs to be accessible from outside
    const users = result.rows.map(user => ({
      ...user,
      photo_url: user.photo_url ? user.photo_url.replace('minio:9000', '72.60.77.160:9000') : null
    }));
    
    res.json(users);
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get user by ID
app.get('/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(
      'SELECT id, name, email, photo_url, created_at FROM users WHERE id = $1',
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    const user = result.rows[0];
    // Transform URL
    if (user.photo_url) {
      user.photo_url = user.photo_url.replace('minio:9000', '72.60.77.160:9000');
    }
    
    res.json(user);
  } catch (error) {
    console.error('Error fetching user:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update user
app.put('/users/:id', upload.single('photo'), validateUser, async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }

  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    const { id } = req.params;
    const { name, email } = req.body;
    
    // Check if user exists
    const userCheck = await client.query('SELECT * FROM users WHERE id = $1', [id]);
    if (userCheck.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    const existingUser = userCheck.rows[0];
    
    // Update user data
    await client.query(
      'UPDATE users SET name = $1, email = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3',
      [name, email, id]
    );
    
    let photoUrl = existingUser.photo_url;
    
    // Upload new photo if provided
    if (req.file) {
      // Delete old photo from MinIO if exists
      if (existingUser.photo_url) {
        try {
          const oldFileName = existingUser.photo_url.split('/').pop();
          await minioClient.removeObject(bucketName, oldFileName);
        } catch (error) {
          console.error('Error deleting old photo:', error);
        }
      }
      
      // Upload new photo
      const photo = await uploadToMinIO(req.file, id);
      photoUrl = photo.url;
      
      await client.query(
        'UPDATE users SET photo_url = $1 WHERE id = $2',
        [photoUrl, id]
      );
    }
    
    await client.query('COMMIT');
    
    res.json({
      id: parseInt(id),
      name,
      email,
      photo_url: photoUrl ? photoUrl.replace('minio:9000', '72.60.77.160:9000') : null
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error updating user:', error);
    
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Email already exists' });
    }
    
    res.status(500).json({ error: 'Internal server error' });
  } finally {
    client.release();
  }
});

// Delete user
app.delete('/users/:id', async (req, res) => {
  const client = await pool.connect();
  
  try {
    const { id } = req.params;
    
    await client.query('BEGIN');
    
    // Get user photo URL
    const userResult = await client.query('SELECT photo_url FROM users WHERE id = $1', [id]);
    
    if (userResult.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    const photoUrl = userResult.rows[0].photo_url;
    
    // Delete from database
    await client.query('DELETE FROM users WHERE id = $1', [id]);
    
    // Delete photo from MinIO if exists
    if (photoUrl) {
      try {
        const fileName = photoUrl.split('/').pop();
        await minioClient.removeObject(bucketName, fileName);
      } catch (error) {
        console.error('Error deleting photo from MinIO:', error);
      }
    }
    
    await client.query('COMMIT');
    
    res.json({ message: 'User deleted successfully' });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error deleting user:', error);
    res.status(500).json({ error: 'Internal server error' });
  } finally {
    client.release();
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date() });
});

// Error handling middleware
app.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File too large. Max size is 5MB' });
    }
    return res.status(400).json({ error: err.message });
  }
  
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
  console.log(`API accessible at http://72.60.77.160:${port}`);
});
