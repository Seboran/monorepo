import NombreNoeud from '../../../../../model/NombreNoeud.model'
import VisiteurNoeud from '../../../../VisiteurNoeud'
import AbstractVisiteurOrchestrateur from '../../AbstractVisiteurOrchestrateur'

export default class JsNombreVisiteur
  extends AbstractVisiteurOrchestrateur<string>
  implements VisiteurNoeud<string, NombreNoeud>
{
  visit(node: NombreNoeud): string {
    return '' + node.value
  }
}
