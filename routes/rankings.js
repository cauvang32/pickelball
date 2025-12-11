import { Router } from 'express'
import { param, validationResult } from 'express-validator'
import { asyncHandler } from '../utils/async-handler.js'

export const createRankingRouter = ({ db, checkAuth, rankingsCache }) => {
  const router = Router()

  // Validation middleware
  const handleValidationErrors = (req, res, next) => {
    const errors = validationResult(req)
    if (!errors.isEmpty()) {
      return res.status(400).json({ error: 'Validation failed', details: errors.array() })
    }
    next()
  }

  router.get('/lifetime', checkAuth, asyncHandler(async (req, res) => {
    const cacheKey = 'rankings:lifetime'
    let rankings = rankingsCache.get(cacheKey)
    let cacheHit = true

    if (!rankings) {
      cacheHit = false
      rankings = await db.getPlayerStatsLifetime()
      rankings = await Promise.all(rankings.map(async (player) => {
        const form = await db.getPlayerForm(player.id, 5)
        return { ...player, form }
      }))
      rankingsCache.set(cacheKey, rankings, 10 * 60 * 1000)
    }

    res.set('X-Cache', cacheHit ? 'HIT' : 'MISS')
    res.set('X-Cache-Key', cacheKey)
    res.json(rankings)
  }))

  router.get('/season/:seasonId', 
    checkAuth,
    [param('seasonId').isInt({ min: 1 }).withMessage('Invalid season ID')],
    handleValidationErrors,
    asyncHandler(async (req, res) => {
    const seasonId = parseInt(req.params.seasonId)
    const cacheKey = `rankings:season:${seasonId}`
    let rankings = rankingsCache.get(cacheKey)
    let cacheHit = true

    if (!rankings) {
      cacheHit = false
      rankings = await db.getPlayerStatsBySeason(seasonId)
      rankings = await Promise.all(rankings.map(async (player) => {
        const form = await db.getPlayerFormBySeason(player.id, seasonId, 5)
        return { ...player, form }
      }))
      rankingsCache.set(cacheKey, rankings, 3 * 60 * 1000)
    }

    res.set('X-Cache', cacheHit ? 'HIT' : 'MISS')
    res.set('X-Cache-Key', cacheKey)
    res.json(rankings)
  }))

  router.get('/date/:date', 
    checkAuth,
    [param('date').isISO8601().withMessage('Invalid date format. Use YYYY-MM-DD')],
    handleValidationErrors,
    asyncHandler(async (req, res) => {
    const { date } = req.params
    const cacheKey = `rankings:date:${date}`
    let rankings = rankingsCache.get(cacheKey)
    let cacheHit = true

    if (!rankings) {
      cacheHit = false
      rankings = await db.getPlayerStatsBySpecificDate(date)
      rankings = await Promise.all(rankings.map(async (player) => {
        const form = await db.getPlayerFormBySpecificDate(player.id, date, 5)
        return { ...player, form }
      }))
      rankingsCache.set(cacheKey, rankings, 15 * 60 * 1000)
    }

    res.set('X-Cache', cacheHit ? 'HIT' : 'MISS')
    res.set('X-Cache-Key', cacheKey)
    res.json(rankings)
  }))

  return router
}
