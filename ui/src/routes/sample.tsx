import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/sample')({
  component: RouteComponent,
})

function RouteComponent() {
  return <div>Hello "/sample"!</div>
}
