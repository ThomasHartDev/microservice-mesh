import { describe, expect, it } from 'vitest'
import { VERSION } from '../src/index.js'

describe('scaffold', () => {
  it('exports a semver version string', () => {
    expect(VERSION).toMatch(/^\d+\.\d+\.\d+$/)
  })
})
