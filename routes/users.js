import { Router } from 'express'
import { body, param } from 'express-validator'
import bcrypt from 'bcryptjs'
import { asyncHandler } from '../utils/async-handler.js'

export const createUserRouter = ({
  db,
  authenticateToken,
  requireAdmin,
  conditionalRateLimit,
  createLimiter,
  deleteLimiter,
  handleValidationErrors
}) => {
  const router = Router()

  // Get all users (admin only)
  router.get(
    '/',
    authenticateToken,
    requireAdmin,
    asyncHandler(async (req, res) => {
      const users = await db.getUsers()
      // Remove password_hash from response
      const safeUsers = users.map(({ password_hash, ...user }) => user)
      res.json(safeUsers)
    })
  )

  // Create new user (admin only)
  router.post(
    '/',
    authenticateToken,
    requireAdmin,
    conditionalRateLimit(createLimiter),
    [
      body('username')
        .isLength({ min: 3, max: 50 })
        .matches(/^[a-zA-Z0-9_]+$/)
        .withMessage('Username must be 3-50 characters, alphanumeric and underscore only'),
      body('password')
        .isLength({ min: 8 })
        .withMessage('Password must be at least 8 characters'),
      body('email')
        .optional({ nullable: true, checkFalsy: true })
        .isEmail()
        .withMessage('Valid email is required'),
      body('role')
        .isIn(['admin', 'editor'])
        .withMessage('Role must be admin or editor')
    ],
    handleValidationErrors,
    asyncHandler(async (req, res) => {
      const { username, password, email, role } = req.body
      const createdBy = req.user.username

      // Check if username already exists
      const existingUser = await db.getUserByUsername(username)
      if (existingUser) {
        return res.status(400).json({ error: 'Username already exists' })
      }

      // Hash password
      const passwordHash = await bcrypt.hash(password, 10)

      const userId = await db.createUser(username, passwordHash, email || null, role, createdBy)
      
      res.json({ 
        success: true, 
        message: 'User created successfully',
        user: { id: userId, username, email, role }
      })
    })
  )

  // Update user (admin only)
  router.put(
    '/:id',
    authenticateToken,
    requireAdmin,
    [
      param('id').isInt().withMessage('Invalid user ID'),
      body('username')
        .optional()
        .isLength({ min: 3, max: 50 })
        .matches(/^[a-zA-Z0-9_]+$/)
        .withMessage('Username must be 3-50 characters, alphanumeric and underscore only'),
      body('email')
        .optional({ nullable: true, checkFalsy: true })
        .isEmail()
        .withMessage('Valid email is required'),
      body('role')
        .optional()
        .isIn(['admin', 'editor'])
        .withMessage('Role must be admin or editor'),
      body('isActive')
        .optional()
        .isBoolean()
        .withMessage('isActive must be boolean')
    ],
    handleValidationErrors,
    asyncHandler(async (req, res) => {
      const userId = parseInt(req.params.id)
      const { username, email, role, isActive } = req.body

      // Get existing user
      const users = await db.getUsers()
      const existingUser = users.find(u => u.id === userId)
      if (!existingUser) {
        return res.status(404).json({ error: 'User not found' })
      }

      // Update user
      await db.updateUser(
        userId,
        username || existingUser.username,
        email !== undefined ? email : existingUser.email,
        role || existingUser.role,
        isActive !== undefined ? isActive : existingUser.is_active
      )

      res.json({ success: true, message: 'User updated successfully' })
    })
  )

  // Change user password (admin only)
  router.put(
    '/:id/password',
    authenticateToken,
    requireAdmin,
    [
      param('id').isInt().withMessage('Invalid user ID'),
      body('password')
        .isLength({ min: 8 })
        .withMessage('Password must be at least 8 characters')
    ],
    handleValidationErrors,
    asyncHandler(async (req, res) => {
      const userId = parseInt(req.params.id)
      const { password } = req.body

      const passwordHash = await bcrypt.hash(password, 10)
      await db.updateUserPassword(userId, passwordHash)

      res.json({ success: true, message: 'Password updated successfully' })
    })
  )

  // Delete user (admin only)
  router.delete(
    '/:id',
    authenticateToken,
    requireAdmin,
    conditionalRateLimit(deleteLimiter),
    [param('id').isInt().withMessage('Invalid user ID')],
    handleValidationErrors,
    asyncHandler(async (req, res) => {
      const userId = parseInt(req.params.id)

      // Prevent deleting self
      const users = await db.getUsers()
      const userToDelete = users.find(u => u.id === userId)
      if (!userToDelete) {
        return res.status(404).json({ error: 'User not found' })
      }
      if (userToDelete.username === req.user.username) {
        return res.status(400).json({ error: 'Cannot delete your own account' })
      }

      await db.deleteUser(userId)
      res.json({ success: true, message: 'User deleted successfully' })
    })
  )

  return router
}
