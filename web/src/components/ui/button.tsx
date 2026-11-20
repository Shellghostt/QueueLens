import type { ComponentProps } from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '../../lib/utils'

const variants = cva('button', { variants: {
  variant: { default: 'button-primary', outline: 'button-outline', ghost: 'button-ghost' },
  size: { default: '', small: 'button-small' },
}, defaultVariants: { variant: 'default', size: 'default' } })

export function Button({ className, variant, size, ...props }: ComponentProps<'button'> & VariantProps<typeof variants>) {
  return <button className={cn(variants({ variant, size }), className)} {...props} />
}
